package scaleset

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
)

func TestRetiringRunnersDoNotSatisfyQueuedDemand(t *testing.T) {
	w := &Worker{
		scaleSet: params.ScaleSet{MaxRunners: 100, DesiredRunnerCount: 3},
		runners: map[string]params.Instance{
			"starting": {Status: commonParams.InstancePendingCreate},
			"creating": {Status: commonParams.InstanceCreating},
			"finished": {Status: commonParams.InstancePendingDelete},
			"forced":   {Status: commonParams.InstancePendingForceDelete},
			"deleting": {Status: commonParams.InstanceDeleting},
			"deleted":  {Status: commonParams.InstanceDeleted},
		},
	}
	if missing := w.targetRunners() - w.runnerCount(); missing != 1 {
		t.Fatalf("queued job must get replacement before provider cleanup; missing=%d", missing)
	}
	// A running worker still satisfies demand, including an active job.
	w.runners["active"] = params.Instance{Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerActive}
	if missing := w.targetRunners() - w.runnerCount(); missing != 0 {
		t.Fatalf("starting and active runners must prevent overprovisioning; missing=%d", missing)
	}
}

func TestDemandUpdateWakesAndCoalesces(t *testing.T) {
	w := &Worker{ctx: context.Background(), scaleSet: params.ScaleSet{Enabled: true, MaxRunners: 100}, scaleUp: make(chan struct{}, 1)}
	for count := 1; count <= 40; count++ {
		w.handleScaleSetEvent(dbCommon.ChangePayload{Operation: dbCommon.UpdateOperation, Payload: params.ScaleSet{Enabled: true, MaxRunners: 100, DesiredRunnerCount: count}})
	}
	if len(w.scaleUp) != 1 || w.targetRunners() != 40 || w.scaleSet.DesiredRunnerCount != 40 {
		t.Fatal("lost demand or notification")
	}
	select {
	case <-w.scaleUp:
	case <-time.After(time.Second):
		t.Fatal("no immediate wake")
	}
}

func TestJITConcurrencyBoundAndStreaming(t *testing.T) {
	entered := make(chan struct{}, 40)
	release := make(chan struct{})
	results := generateJITConfigs(context.Background(), 40, func(context.Context) (jitResult, error) {
		entered <- struct{}{}
		<-release
		return jitResult{}, nil
	})
	for n := 0; n < jitConcurrency; n++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("requests serialized")
		}
	}
	select {
	case <-entered:
		t.Fatal("exceeded concurrency bound")
	default:
	}
	release <- struct{}{}
	select {
	case <-results:
	case <-time.After(time.Second):
		t.Fatal("result held until batch completion")
	}
	close(release)
	count := 1
	for range results {
		count++
	}
	if count != 40 {
		t.Fatalf("got %d results", count)
	}
}

func TestJITStopsSchedulingAfterError(t *testing.T) {
	var calls atomic.Int32
	for range generateJITConfigs(context.Background(), 40, func(context.Context) (jitResult, error) {
		calls.Add(1)
		return jitResult{}, errors.New("rate limited")
	}) {
	}
	if calls.Load() > jitConcurrency {
		t.Fatalf("scheduled %d calls after error", calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range generateJITConfigs(ctx, 40, func(context.Context) (jitResult, error) {
		t.Error("called after cancellation")
		return jitResult{}, nil
	}) {
	}
}

func TestJITFailureCancelsWorkerPausedBeforeHTTPDispatch(t *testing.T) {
	var dispatched atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	var calls atomic.Int32
	paused := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := generateJITConfigs(ctx, 2, func(requestCtx context.Context) (jitResult, error) {
		if calls.Add(1) == 1 {
			<-paused
			return jitResult{}, errors.New("rate limited")
		}
		// This worker passed the scheduling check before the other request failed.
		close(paused)
		<-release
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, server.URL, nil)
		if err != nil {
			return jitResult{}, err
		}
		resp, err := server.Client().Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		return jitResult{}, err
	})
	// Receiving the first failure establishes that cancellation was published
	// before the paused worker resumes dispatching its HTTP request.
	first := <-results
	if first.err == nil {
		t.Fatal("expected first request failure")
	}
	close(release)
	second := <-results
	if !errors.Is(second.err, context.Canceled) {
		t.Fatalf("paused request was not cancelled: %v", second.err)
	}
	for range results {
	}
	if dispatched.Load() != 0 {
		t.Fatal("HTTP request dispatched after failure")
	}
}

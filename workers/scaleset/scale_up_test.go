package scaleset

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
)

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
	results := generateJITConfigs(context.Background(), 40, func() (jitResult, error) {
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
	for range generateJITConfigs(context.Background(), 40, func() (jitResult, error) {
		calls.Add(1)
		return jitResult{}, errors.New("rate limited")
	}) {
	}
	if calls.Load() > jitConcurrency {
		t.Fatalf("scheduled %d calls after error", calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range generateJITConfigs(ctx, 40, func() (jitResult, error) { t.Error("called after cancellation"); return jitResult{}, nil }) {
	}
}

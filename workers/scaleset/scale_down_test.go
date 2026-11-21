package scaleset

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/cache"
	dbCommon "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-github/v84/github"
)

// Stop at the first remote side effect, before a real token, HTTP call or DB
// mutation. Idle runners must still reach this boundary; booting ones must not.
type scaleDownClient struct {
	common.GithubClient
	registrationCalls int
	baseURL           *url.URL
}

var scaleDownLocker sync.Once

func (c *scaleDownClient) GetEntity() params.ForgeEntity {
	return params.ForgeEntity{ID: "scale-down-test", EntityType: params.ForgeEntityTypeOrganization}
}

func (c *scaleDownClient) CreateEntityRegistrationToken(context.Context) (*github.RegistrationToken, *github.Response, error) {
	c.registrationCalls++
	if c.baseURL != nil {
		return &github.RegistrationToken{Token: github.Ptr("test-token"), ExpiresAt: &github.Timestamp{Time: time.Now().Add(time.Hour)}}, nil, nil
	}
	return nil, nil, errors.New("test stops removal before HTTP")
}

func TestScaleDownPreservesBootstrapUntilRunnerBecomesIdle(t *testing.T) {
	scaleDownLocker.Do(func() {
		locker, err := locking.NewLocalLocker(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := locking.RegisterLocker(locker); err != nil {
			t.Fatal(err)
		}
	})
	client := &scaleDownClient{}
	cache.SetGithubClient("scale-down-test", client)
	t.Cleanup(func() { cache.DeleteGithubClient("scale-down-test") })
	w := &Worker{
		ctx: context.Background(), consumerID: "scale-down-test",
		scaleSet: params.ScaleSet{OrgID: "scale-down-test", MaxRunners: 10, DesiredRunnerCount: 1},
		runners: map[string]params.Instance{
			"finishing": {ID: "finishing", Name: "finishing", Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerActive},
		},
	}
	// A demand update can precede the completion/cleanup of the previous job.
	// The apparent surplus must not destroy a replacement that is still booting.
	for _, status := range []params.RunnerStatus{params.RunnerPending, params.RunnerInstalling} {
		w.runners["replacement"] = params.Instance{ID: "replacement", Name: "replacement", Status: commonParams.InstanceRunning, RunnerStatus: status}
		w.handleScaleDown()
		if client.registrationCalls != 0 {
			t.Fatalf("scale-down attempted to remove a %s runner before bootstrap completed", status)
		}
	}
	// Once the callback reports idle, the same surplus reaches the GitHub
	// readiness check; it is no longer rejected solely on local bootstrap state.
	runner := w.runners["replacement"]
	runner.RunnerStatus = params.RunnerIdle
	w.runners["replacement"] = runner
	w.handleScaleDown()
	if client.registrationCalls != 1 {
		t.Fatalf("idle surplus did not reach readiness check: calls=%d", client.registrationCalls)
	}
}

func (c *scaleDownClient) GithubBaseURL() *url.URL { return c.baseURL }

type scaleDownStore struct {
	dbCommon.Store
	runner  params.Instance
	updates int
}

func (s *scaleDownStore) UpdateInstance(_ context.Context, name string, update params.UpdateInstanceParams) (params.Instance, error) {
	s.updates++
	s.runner.Status = update.Status
	return s.runner, nil
}

// Exercise the real Actions client and DELETE path, with only GitHub and the
// database replaced. The bootstrap callback has already marked this runner idle.
func TestScaleDownChecksLiveReadiness(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   any
		busy     bool
		response int
		remove   bool
	}{
		{name: "offline bootstrap", status: "offline"},
		{name: "busy despite stale idle callback", status: "online", busy: true},
		{name: "unknown status", status: 99},
		{name: "read failed", response: http.StatusServiceUnavailable},
		{name: "online idle surplus", status: "online", remove: true},
		{name: "registration already absent", response: http.StatusNotFound, remove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, store, deletes := newReadinessWorker(t, tc.status, tc.busy, tc.response)
			w.handleScaleDown()
			want := 0
			if tc.remove {
				want = 1
			}
			if int(deletes.Load()) != want || store.updates != want {
				t.Fatalf("DELETEs=%d DB updates=%d, want %d", deletes.Load(), store.updates, want)
			}
			if tc.remove && w.runners["readiness"].Status != commonParams.InstancePendingDelete {
				t.Fatal("surplus runner was not queued for provider cleanup")
			}
		})
	}
}

func TestIdleBootstrapTimeoutStillReapsWithoutSurplus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		age    time.Duration
		status string
		busy   bool
		remove bool
	}{
		{"young offline", time.Minute, "offline", false, false},
		{"expired offline", 11 * time.Minute, "offline", false, true},
		{"expired online idle", 11 * time.Minute, "online", false, false},
		{"expired busy", 11 * time.Minute, "online", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, store, deletes := newReadinessWorker(t, tc.status, tc.busy, 0)
			w.scaleSet.DesiredRunnerCount = 1
			runner := w.runners["readiness"]
			runner.CreatedAt = time.Now().Add(-tc.age)
			w.runners[runner.ID] = runner
			unlock, err := w.reapTimedOutRunners(map[string]params.RunnerReference{
				runner.Name: {ID: runner.AgentID, Name: runner.Name, Status: tc.status, Busy: tc.busy},
			})
			if err != nil {
				t.Fatal(err)
			}
			unlock()
			want := 0
			if tc.remove {
				want = 1
			}
			if int(deletes.Load()) != want || store.updates != want {
				t.Fatalf("DELETEs=%d DB updates=%d, want %d", deletes.Load(), store.updates, want)
			}
		})
	}
}

func newReadinessWorker(t *testing.T, status any, busy bool, response int) (*Worker, *scaleDownStore, *atomic.Int32) {
	t.Helper()
	scaleDownLocker.Do(func() {
		locker, err := locking.NewLocalLocker(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := locking.RegisterLocker(locker); err != nil {
			t.Fatal(err)
		}
	})
	deletes := &atomic.Int32{}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}).SignedString([]byte("test-only"))
	if err != nil {
		t.Fatal(err)
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/actions/runner-registration":
			_ = json.NewEncoder(rw).Encode(params.ActionsServiceAdminInfoResponse{URL: server.URL, Token: token})
		case req.URL.Path != "/_apis/distributedtask/pools/0/agents/42":
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
			rw.WriteHeader(http.StatusNotFound)
		case req.Method == http.MethodGet:
			if response != 0 {
				rw.WriteHeader(response)
				return
			}
			_ = json.NewEncoder(rw).Encode(params.RunnerReference{ID: 42, Name: "readiness", Status: status, Busy: busy})
		case req.Method == http.MethodDelete:
			deletes.Add(1)
			rw.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", req.Method)
			rw.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cache.SetGithubClient("scale-down-test", &scaleDownClient{baseURL: baseURL})
	t.Cleanup(func() { cache.DeleteGithubClient("scale-down-test") })
	runner := params.Instance{ID: "readiness", Name: "readiness", AgentID: 42, CreatedAt: time.Now(), Status: commonParams.InstanceRunning, RunnerStatus: params.RunnerIdle}
	store := &scaleDownStore{runner: runner}
	return &Worker{
		ctx: context.Background(), consumerID: "readiness-test", store: store,
		scaleSet: params.ScaleSet{OrgID: "scale-down-test", MaxRunners: 10, RunnerBootstrapTimeout: 10},
		runners:  map[string]params.Instance{runner.ID: runner},
	}, store, deletes
}

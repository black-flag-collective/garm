package scaleset

import (
	"context"
	"errors"
	"sync"
	"testing"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/cache"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common"
	"github.com/google/go-github/v84/github"
)

// Stop at the first remote side effect, before a real token, HTTP call or DB
// mutation. Idle runners must still reach this boundary; booting ones must not.
type scaleDownClient struct {
	common.GithubClient
	registrationCalls int
}

var scaleDownLocker sync.Once

func (c *scaleDownClient) GetEntity() params.ForgeEntity {
	return params.ForgeEntity{ID: "scale-down-test", EntityType: params.ForgeEntityTypeOrganization}
}

func (c *scaleDownClient) CreateEntityRegistrationToken(context.Context) (*github.RegistrationToken, *github.Response, error) {
	c.registrationCalls++
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
	// Once bootstrap finishes, the same surplus remains eligible for ordinary
	// scale-down. This does not turn zero-idle capacity into a permanent reserve.
	runner := w.runners["replacement"]
	runner.RunnerStatus = params.RunnerIdle
	w.runners["replacement"] = runner
	w.handleScaleDown()
	if client.registrationCalls != 1 {
		t.Fatalf("idle surplus did not reach removal: calls=%d", client.registrationCalls)
	}
}

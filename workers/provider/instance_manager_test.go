package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	commonParams "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

func TestInstanceManagerReconcilesImmediately(t *testing.T) {
	// Deleted is a terminal state which needs no provider or helper. The real
	// reconciliation loop must observe it without waiting for its five-second tick.
	i := &instanceManager{ctx: context.Background(), instance: params.Instance{Status: commonParams.InstanceDeleted}}
	if err := i.Start(); err != nil {
		t.Fatal(err)
	}
	defer i.Stop()
	select {
	case <-i.quit:
	case <-time.After(time.Second):
		t.Fatal("initial reconciliation waited for ticker")
	}
}

type immediateHelper struct {
	providerHelper
	called chan commonParams.InstanceStatus
}

func (h *immediateHelper) SetInstanceStatus(_ string, status commonParams.InstanceStatus, _ []byte, _ bool) error {
	h.called <- status
	return errors.New("stop before provider call")
}
func TestPendingCreateStartsImmediately(t *testing.T) {
	helper := &immediateHelper{called: make(chan commonParams.InstanceStatus, 1)}
	i := &instanceManager{ctx: context.Background(), helper: helper, instance: params.Instance{Status: commonParams.InstancePendingCreate}}
	if err := i.Start(); err != nil {
		t.Fatal(err)
	}
	defer i.Stop()
	select {
	case status := <-helper.called:
		if status != commonParams.InstanceCreating {
			t.Fatalf("unexpected status %s", status)
		}
	case <-time.After(time.Second):
		t.Fatal("pending instance waited for ticker")
	}
}

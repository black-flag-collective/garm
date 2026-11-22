package scaleset

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cloudbase/garm/params"
)

func TestCompletionTelemetry(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	finish := time.Date(2026, 11, 22, 1, 2, 3, 0, time.UTC)
	for _, result := range []string{"success", "failure", "canceled", ""} {
		output.Reset()
		logJobCompleted(context.Background(), params.ScaleSetJobMessage{
			JobID: "job-123", JobDisplayName: "build", RunnerName: "runner-1",
			Result: result, OwnerName: "example", RepositoryName: "repo",
			WorkflowRunID: 123, RunnerRequestID: 456, FinishTime: finish,
			AcquireJobURL:  "https://broker.invalid/acquire?token=SECRET",
			JobWorkflowRef: "PRIVATE_WORKFLOW", RequestLabels: []string{"PRIVATE_LABEL"},
		})
		var got map[string]any
		if err := json.Unmarshal(output.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		expected := map[string]any{"msg": "job completed", "level": "INFO", "job_id": "job-123", "job_name": "build", "runner_name": "runner-1", "owner": "example", "repository": "repo", "run_id": float64(123), "request_id": float64(456), "finish_time": finish.Format(time.RFC3339)}
		if result != "" {
			expected["result"] = result
		}
		for key, value := range expected {
			if got[key] != value {
				t.Errorf("%s: got %v, want %v", key, got[key], value)
			}
		}
		delete(got, "time")
		if len(got) != len(expected) {
			t.Errorf("unexpected fields: %v", got)
		}
		for _, forbidden := range []string{"SECRET", "https://", "PRIVATE_WORKFLOW", "PRIVATE_LABEL", "acquireJobUrl"} {
			if strings.Contains(output.String(), forbidden) {
				t.Errorf("leaked %s", forbidden)
			}
		}
	}
	output.Reset()
	logJobCompleted(context.Background(), params.ScaleSetJobMessage{})
	var got map[string]any
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"result", "owner", "repository", "run_id", "request_id", "finish_time"} {
		if _, exists := got[key]; exists {
			t.Errorf("absent field %s emitted", key)
		}
	}
}

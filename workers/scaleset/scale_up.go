package scaleset

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cloudbase/garm/params"
)

const jitConcurrency = 8

type jitResult struct {
	name   string
	config params.RunnerScaleSetJitRunnerConfig
	err    error
}

// requestScaleUp coalesces notifications. It is called with mux held and the
// channel is never closed, so shutdown cannot race a database notification.
func (w *Worker) requestScaleUp() {
	select {
	case w.scaleUp <- struct{}{}:
	default:
	}
}

// generateJITConfigs streams successful requests as soon as they finish. Stop
// scheduling on the first error (including throttling), draining in-flight
// successes so their GitHub runners are not orphaned.
func generateJITConfigs(ctx context.Context, count int, generate func() (jitResult, error)) <-chan jitResult {
	results := make(chan jitResult, jitConcurrency)
	var next atomic.Int64
	var failed atomic.Bool
	var wg sync.WaitGroup
	for n := 0; n < min(count, jitConcurrency); n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !failed.Load() && ctx.Err() == nil {
				if int(next.Add(1)) > count {
					return
				}
				result, err := generate()
				result.err = err
				if err != nil {
					failed.Store(true)
				}
				results <- result
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()
	return results
}

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
// successes so their GitHub runners are not orphaned. Pass the cancellation
// context through to HTTP: a worker paused after its loop check must not
// dispatch a request after another worker reports an error.
func generateJITConfigs(ctx context.Context, count int, generate func(context.Context) (jitResult, error)) <-chan jitResult {
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan jitResult, jitConcurrency)
	var next atomic.Int64
	var wg sync.WaitGroup
	for n := 0; n < min(count, jitConcurrency); n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if int(next.Add(1)) > count {
					return
				}
				result, err := generate(ctx)
				result.err = err
				if err != nil {
					cancel()
				}
				results <- result
			}
		}()
	}
	go func() { wg.Wait(); cancel(); close(results) }()
	return results
}

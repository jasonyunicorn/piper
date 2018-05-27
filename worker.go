package piper

import (
	"context"
)

// status is a struct used to communicate the results of a worker's last batch job
type status struct {
	address chan *batch // worker's channel to send new batches
	results *batch      // results of the previously run batch
}

// worker is an object used execute the user-defined batch executable callback function
type worker struct {
	// required
	batchFn  BatchExecFn  // batch function to call on incoming batch jobs
	statusCh chan *status // channel used to report results of the previous batch

	// internal
	exec    executable    // mechanism for starting / stopping
	batchCh chan *batch   // channel used to receive incoming batches
	stopCh  chan struct{} // channel used to gracefully terminate a worker
}

// newWorker creates a pointer to a worker
func newWorker(fn BatchExecFn, statusCh chan *status) *worker {
	w := worker{
		batchFn:  fn,
		statusCh: statusCh,
	}
	w.exec = newExec(w.startFn, w.stopFn)

	return &w
}

// startFn defines the startup procedure for a worker
func (w *worker) startFn(ctx context.Context) {
	// Initialize channels
	w.batchCh = make(chan *batch)
	w.stopCh = make(chan struct{})

	go func() {
		var batch *batch

	work:
		for {
			select {
			// Handle context cancellation
			case <-ctx.Done():
				break work
			// Handle requests to stop work through the stopCh
			case <-w.stopCh:
				break work
			// Send a status report to the dispatcher to communicate previous failures
			// At startup batch is nil, indicating no previous failures
			case w.statusCh <- &status{
				address: w.batchCh,
				results: batch,
			}:
			// Receive a new batch from the dispatcher and execute the user-supplied batch callback function
			case batch = <-w.batchCh:
				batch.execute(ctx, w.batchFn)
			}
		}
	}()
}

// stopFn defines the shutdown procedure for a worker
func (w *worker) stopFn(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case w.stopCh <- struct{}{}:
		close(w.stopCh)
		close(w.batchCh)
	}
}

package piper

import (
	"context"
	"testing"
	"time"
)

type testDispatcher struct {
	statusCh    chan *status
	batchExecFn BatchExecFn
}

func newTestDispatcher() *testDispatcher {
	fn := testBatchExecEvensFailFn{}
	return &testDispatcher{
		batchExecFn: fn.Execute,
		statusCh:    make(chan *status),
	}
}

func TestWorker_NewWorker(t *testing.T) {
	td := newTestDispatcher()
	w := newWorker(td.batchExecFn, td.statusCh)
	if w == nil {
		t.Fatal("newWorker returned nil")
	}
}

func TestWorker_StartDispatchStop(t *testing.T) {
	td := newTestDispatcher()
	w := newWorker(td.batchExecFn, td.statusCh)

	ctx := context.TODO()
	w.exec.start(ctx)

	// Create some jobs and put it in a batch
	numJobs := 10
	b := newBatch(numJobs)
	js := newTestJobs(numJobs)
	b.add(js...)

	// Wait for a status report from the worker
	status := <-w.statusCh

	// Send the batch
	status.address <- b

	w.exec.stop(ctx)
}

func TestWorker_CancelContext1(t *testing.T) {
	td := newTestDispatcher()
	w := newWorker(td.batchExecFn, td.statusCh)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.exec.start(ctx)
	w.exec.stop(ctx)
}

func TestWorker_CancelContext2(t *testing.T) {
	td := newTestDispatcher()
	w := newWorker(td.batchExecFn, td.statusCh)

	ctx, cancel := context.WithCancel(context.Background())
	w.exec.start(ctx)
	<-time.After(100 * time.Millisecond)
	cancel()
	w.exec.stop(ctx)
}

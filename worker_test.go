package piper

import (
	"context"
	"sync"
	"testing"
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
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go w.exec.start(context.TODO(), wg)
	wg.Wait()

	defer func(wg *sync.WaitGroup) {
		wg.Add(1)
		go w.exec.stop(context.TODO(), wg)
		wg.Wait()
	}(wg)

	// Create some jobs and put it in a batch
	numJobs := 10
	b := newBatch(numJobs)
	js := newTestJobs(numJobs)
	b.add(js...)

	// Wait for a status report from the worker
	status := <-w.statusCh

	// Send the batch
	status.address <- b
}

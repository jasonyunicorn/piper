package piper

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type testProcess struct {
	successCount *uint64
	failureCount *uint64
	onSuccessFn  ProcessFn
	onFailureFn  ProcessFn
}

func newTestProcess() *testProcess {
	var successCount, failureCount uint64 = 0, 0
	onSuccessFn := func(d DataIF) []DataIF {
		atomic.AddUint64(&successCount, 1)
		return []DataIF{d}
	}
	onFailureFn := func(d DataIF) []DataIF {
		atomic.AddUint64(&failureCount, 1)
		return []DataIF{d}
	}

	return &testProcess{
		successCount: &successCount,
		failureCount: &failureCount,
		onSuccessFn:  onSuccessFn,
		onFailureFn:  onFailureFn,
	}
}

func TestProcess_NewProcess(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te)
	if p == nil {
		t.Fatal("NewProcess returned nil")
	}
}

func TestProcess_ProcessWithConcurrency(t *testing.T) {
	want := 10
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithConcurrency(want))
	got := p.concurrency
	if want != got {
		t.Fatalf("concurrency invalid: want [%d], got [%d]", want, got)
	}
}

func TestProcess_ProcessWithQueueDepth(t *testing.T) {
	want := 100
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithQueueDepth(want))
	got := p.queueDepth
	if want != got {
		t.Fatalf("queueDepth invalid: want [%d], got [%d]", want, got)
	}
}

func TestProcess_ProcessWithBatchTimeout(t *testing.T) {
	want := 2 * time.Second
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithBatchTimeout(want))
	got := p.batchTimeout
	if want != got {
		t.Fatalf("batchTimeout invalid: want [%v], got [%v]", want, got)
	}
}

func TestProcess_ProcessWithMaxBatchSize(t *testing.T) {
	want := 1000
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithMaxBatchSize(want))
	got := p.maxBatchSize
	if want != got {
		t.Fatalf("maxBatchSize invalid: want [%d], got [%d]", want, got)
	}
}

func TestProcess_ProcessWithMaxRetries(t *testing.T) {
	want := 3
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithMaxRetries(want))
	got := p.maxRetries
	if want != got {
		t.Fatalf("maxRetries invalid: want [%d], got [%d]", want, got)
	}
}

func TestProcess_ProcessWithRateLimit(t *testing.T) {
	want := rate.Inf
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithRateLimit(want))
	got := p.rateLimit
	if want != got {
		t.Fatalf("maxRetries invalid: want [%v], got [%v]", want, got)
	}
}

func TestProcess_ProcessWithOnSuccessFns(t *testing.T) {
	fn1 := func(d DataIF) []DataIF { return []DataIF{d} }
	fn2 := func(d DataIF) []DataIF { return []DataIF{d} }
	fns := []ProcessFn{fn1, fn2}
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithOnSuccessFns(fns...))
	if len(p.onSuccessFns) != len(fns) {
		t.Fatal("onSuccessFns invalid")
	}
}

func TestProcess_ProcessWithOnFailureFns(t *testing.T) {
	fn1 := func(d DataIF) []DataIF { return []DataIF{} }
	fn2 := func(d DataIF) []DataIF { return []DataIF{} }
	fns := []ProcessFn{fn1, fn2}
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithOnFailureFns(fns...))
	if len(p.onFailureFns) != len(fns) {
		t.Fatal("onFailureFns invalid")
	}
}

func TestProcess_PushOnSuccessFns(t *testing.T) {
	fn1 := func(d DataIF) []DataIF { return []DataIF{} }
	fn2 := func(d DataIF) []DataIF { return []DataIF{} }
	fns := []ProcessFn{fn1, fn2}
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithOnSuccessFns(fns[0]))
	p.pushOnSuccessFns(fns[1])
	if len(p.onSuccessFns) != len(fns) {
		t.Fatal("onSuccessFns invalid after pushOnSuccessFns")
	}
}

func TestProcess_PushOnFailureFns(t *testing.T) {
	fn1 := func(d DataIF) []DataIF { return []DataIF{} }
	fn2 := func(d DataIF) []DataIF { return []DataIF{} }
	fns := []ProcessFn{fn1, fn2}
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess", &te, ProcessWithOnFailureFns(fns[0]))
	p.pushOnFailureFns(fns[1])
	if len(p.onFailureFns) != len(fns) {
		t.Fatal("onFailureFns invalid after pushOnFailureFns")
	}
}

func TestProcess_StartStop(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess - Start/Stop", &te)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go p.Start(context.TODO(), wg)
	wg.Wait()

	wg.Add(1)
	go p.Stop(context.TODO(), wg)
	wg.Wait()
}

func TestProcess_ProcessData1(t *testing.T) {
	want := 100
	datum := newTestDatum(want)

	tp := newTestProcess()
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess - Evens Fail, Odds Succeed", &te,
		ProcessWithOnSuccessFns(tp.onSuccessFn),
		ProcessWithOnFailureFns(tp.onFailureFn),
		ProcessWithMaxRetries(0),
		ProcessWithBatchTimeout(500*time.Millisecond),
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go p.Start(context.TODO(), wg)
	wg.Wait()

	for _, data := range datum {
		p.ProcessData(data)
	}

	time.Sleep(3 * time.Second)
	wg.Add(1)
	go p.Stop(context.TODO(), wg)
	wg.Wait()

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != want {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", want, got)
	}
}

func TestProcess_ProcessData2(t *testing.T) {
	retries := 1
	dataCount := 100
	datum := newTestDatum(100)

	tp := newTestProcess()
	te := testBatchExecAllFailFn{}

	p := NewProcess("TestProcess - Retries", &te,
		ProcessWithOnSuccessFns(tp.onSuccessFn),
		ProcessWithOnFailureFns(tp.onFailureFn),
		ProcessWithMaxRetries(retries),
		ProcessWithBatchTimeout(500*time.Millisecond),
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go p.Start(context.TODO(), wg)
	wg.Wait()

	for _, data := range datum {
		p.ProcessData(data)
	}

	time.Sleep(6 * time.Second)
	wg.Add(1)
	go p.Stop(context.TODO(), wg)
	wg.Wait()

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	if gotSuccessCount != 0 {
		t.Fatalf("ProccessData successCount invalid result: want [%d], got [%d]", 0, gotSuccessCount)
	}
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	want := uint64(dataCount)
	if gotFailureCount != want {
		t.Fatalf("ProccessData failureCount invalid result: want [%d], got [%d]", want, gotFailureCount)
	}
}

func newTestDatum(count int) []DataIF {
	datum := make([]DataIF, 0)
	for i := 0; i < count; i++ {
		datum = append(datum, &testData{
			id:    strconv.Itoa(i),
			value: i,
		})
	}

	return datum
}

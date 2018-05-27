package piper

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeline_NewPipeline(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	proc1 := NewProcess("TestProcess1", &te)
	proc2 := NewProcess("TestProcess2", &te)

	// Test 0 Processes
	var processes []*Process = make([]*Process, 0)
	p0, err := NewPipeline("TestPipeline", processes)
	if p0 != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
	if err == nil {
		t.Fatal("NewPipeline expected error but nil")
	}

	// Test 1 Process
	processes = append(processes, proc1)
	p1, err := NewPipeline("TestPipeline", processes)
	if p1 != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
	if err == nil {
		t.Fatal("NewPipeline expected error but nil")
	}

	// Test 2 Processes
	processes = append(processes, proc2)
	p2, err := NewPipeline("TestPipeline", processes)
	if p2 == nil {
		t.Fatal("NewPipeline returned nil")
	}
	if err != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
}

func TestPipeline_PipelineWithWaitGroup(t *testing.T) {
	want := &sync.WaitGroup{}
	te := testBatchExecEvensFailFn{}
	proc1 := NewProcess("TestProcess1", &te)
	proc2 := NewProcess("TestProcess2", &te)

	var processes []*Process = make([]*Process, 0)
	processes = append(processes, proc1)
	processes = append(processes, proc2)

	p, err := NewPipeline("TestPipeline", processes, PipelineWithWaitGroup(want))
	if err != nil {
		t.Fatal("NewPipeline expected nil but not")
	}

	got := p.wg
	if want != got {
		t.Fatalf("WaitGroup invalid: want [%v], got [%v]", want, got)
	}
}

func TestPipeline_StartStop(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	proc1 := NewProcess("TestProcess1", &te)
	proc2 := NewProcess("TestProcess2", &te)

	processes := []*Process{proc1, proc2}
	p, _ := NewPipeline("TestPipeline", processes)

	ctx := context.TODO()
	p.Start(ctx)
	p.Stop(ctx)
}

func TestPipeline_ProcessData(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	wg := &sync.WaitGroup{}
	tp := newTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 2
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 2 Processes, Supply WaitGroup", processes,
		PipelineWithWaitGroup(wg),
	)

	ctx := context.TODO()
	p.Start(ctx)
	wg.Add(1)
	go func(wg *sync.WaitGroup, datum []DataIF) {
		defer wg.Done()
		for _, data := range datum {
			p.ProcessData(data)
		}
	}(wg, datum)
	wg.Wait()
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

func TestPipeline_ProcessDataAsync(t *testing.T) {
	dataCount := 1
	datum := newTestDatum(dataCount)

	tp := newTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 2
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
			ProcessWithMaxBatchSize(1),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 2 Processes", processes)

	ctx := context.TODO()
	p.Start(ctx)
	p.ProcessDataAsync(datum[0])
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

func TestPipeline_ProcessDatum(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	wg := &sync.WaitGroup{}
	tp := newTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 2
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 2 Processes, Supply WaitGroup", processes,
		PipelineWithWaitGroup(wg),
	)

	ctx := context.TODO()
	p.Start(ctx)
	wg.Add(1)
	go func(wg *sync.WaitGroup, datum []DataIF) {
		defer wg.Done()
		p.ProcessDatum(datum)
	}(wg, datum)
	wg.Wait()
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

func TestPipeline_ProcessDatumAsync1(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	tp := newTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 2
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 2 Processes", processes)

	ctx := context.TODO()
	p.Start(ctx)
	p.ProcessDatumAsync(datum)
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

func TestPipeline_ProcessDatumAsync2(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	tp := newTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 3
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 3 Processes", processes)

	ctx := context.TODO()
	p.Start(ctx)
	p.ProcessDatumAsync(datum)
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

func TestPipeline_ProcessDatumAsync3(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	tp := newExpandingTestProcess()
	te := testBatchExecAllSucceedFn{}

	numProcesses := 3
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
			ProcessWithMaxBatchSize(1),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 3 Processes, Expanding", processes)

	ctx := context.TODO()
	p.Start(ctx)
	p.ProcessDatumAsync(datum)
	p.Stop(ctx)

	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)

	var factor uint
	var i uint
	for i = 0; i < uint(numProcesses); i++ {
		factor += 1 << i
	}
	want := dataCount * int(factor)
	if got != want {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", want, got)
	}
}

package batch

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeline_NewPipeline(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	proc1 := NewProcess("TestProcess1", &te)
	proc2 := NewProcess("TestProcess2", &te)

	// Test 0 Processes
	p0, err := NewPipeline("TestPipeline")
	if p0 != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
	if err == nil {
		t.Fatal("NewPipeline expected error but nil")
	}

	// Test 1 Process
	p1, err := NewPipeline("TestPipeline", proc1)
	if p1 != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
	if err == nil {
		t.Fatal("NewPipeline expected error but nil")
	}

	// Test 2 Processes
	p2, err := NewPipeline("TestPipeline", proc1, proc2)
	if p2 == nil {
		t.Fatal("NewPipeline returned nil")
	}
	if err != nil {
		t.Fatal("NewPipeline expected nil but not")
	}
}

func TestPipeline_StartStop(t *testing.T) {
	te := testBatchExecEvensFailFn{}
	proc1 := NewProcess("TestProcess1", &te)
	proc2 := NewProcess("TestProcess2", &te)

	p, _ := NewPipeline("TestPipeline", proc1, proc2)
	p.Start(context.TODO())
	p.Stop(context.TODO())
}

func TestPipeline_ProcessData1(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	tp := newTestProcess()

	numProcesses := 2
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		te := testBatchExecAllSucceedFn{}
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 2 Pipelines", processes...)
	p.Start(context.TODO())
	defer p.Stop(context.TODO())

	for _, data := range datum {
		p.ProcessData(data)
	}

	time.Sleep(10 * time.Second)
	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}

}

func TestPipeline_ProcessData2(t *testing.T) {
	dataCount := 100
	datum := newTestDatum(dataCount)

	tp := newTestProcess()

	numProcesses := 3
	processes := make([]*Process, numProcesses)
	for i := 0; i < numProcesses; i++ {
		te := testBatchExecAllSucceedFn{}
		processes[i] = NewProcess(fmt.Sprintf("TestProcess#%s", strconv.Itoa(i+1)), &te,
			ProcessWithOnSuccessFns(tp.onSuccessFn),
			ProcessWithOnFailureFns(tp.onFailureFn),
			ProcessWithMaxRetries(0),
			ProcessWithBatchTimeout(500*time.Millisecond),
		)
	}
	p, _ := NewPipeline("TestPipeline - All Jobs Succeed, 3 Pipelines", processes...)
	p.Start(context.TODO())
	defer p.Stop(context.TODO())

	for _, data := range datum {
		p.ProcessData(data)
	}

	time.Sleep(10 * time.Second)
	gotSuccessCount := atomic.LoadUint64(tp.successCount)
	gotFailureCount := atomic.LoadUint64(tp.failureCount)
	got := int(gotSuccessCount) + int(gotFailureCount)
	if got != dataCount*numProcesses {
		t.Fatalf("ProccessData invalid result: want [%d], got [%d]", dataCount*numProcesses, got)
	}
}

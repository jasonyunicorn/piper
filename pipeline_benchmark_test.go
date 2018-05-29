package piper

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func BenchmarkPipeline_Start(b *testing.B) {
	b.ReportAllocs()

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
	b.ResetTimer()
	b.StartTimer()
	p.Start(ctx)
	b.StopTimer()
	p.Stop(ctx)
}

func BenchmarkPipeline_Stop(b *testing.B) {
	b.ReportAllocs()

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
	b.ResetTimer()
	b.StartTimer()
	p.Stop(ctx)
	b.StopTimer()
}

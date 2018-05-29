package piper

import (
	"context"
	"testing"
	"time"
)

func BenchmarkProcess_Start(b *testing.B) {
	b.ReportAllocs()

	tp := newTestProcess()
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess - Evens Fail, Odds Succeed", &te,
		ProcessWithOnSuccessFns(tp.onSuccessFn),
		ProcessWithOnFailureFns(tp.onFailureFn),
		ProcessWithMaxRetries(0),
		ProcessWithBatchTimeout(500*time.Millisecond),
	)

	ctx := context.TODO()
	b.ResetTimer()
	b.StartTimer()
	p.Start(ctx)
	b.StopTimer()
	p.Stop(ctx)
}

func BenchmarkProcess_Stop(b *testing.B) {
	b.ReportAllocs()

	tp := newTestProcess()
	te := testBatchExecEvensFailFn{}
	p := NewProcess("TestProcess - Evens Fail, Odds Succeed", &te,
		ProcessWithOnSuccessFns(tp.onSuccessFn),
		ProcessWithOnFailureFns(tp.onFailureFn),
		ProcessWithMaxRetries(0),
		ProcessWithBatchTimeout(500*time.Millisecond),
	)

	ctx := context.TODO()
	p.Start(ctx)
	b.ResetTimer()
	b.StartTimer()
	p.Stop(ctx)
	b.StopTimer()
}

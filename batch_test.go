package piper

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

type testBatchExecAllSucceedFn struct {
}

func (fn *testBatchExecAllSucceedFn) Execute(ctx context.Context, datum []DataIF) (map[string]error, error) {
	errorsMap := make(map[string]error)
	for _, data := range datum {
		td := data.(*testData)
		errorsMap[td.id] = nil
	}
	return errorsMap, nil
}

type testBatchExecAllFailFn struct {
}

func (fn *testBatchExecAllFailFn) Execute(ctx context.Context, datum []DataIF) (map[string]error, error) {
	errorsMap := make(map[string]error)
	for _, data := range datum {
		td := data.(*testData)
		errorsMap[td.id] = fmt.Errorf("Error#%d", td.value)
	}
	return errorsMap, nil
}

type testBatchExecEvensFailFn struct {
}

func (fn *testBatchExecEvensFailFn) Execute(ctx context.Context, datum []DataIF) (map[string]error, error) {
	errorsMap := make(map[string]error)
	for _, data := range datum {
		td := data.(*testData)
		if td.value%2 == 0 {
			errorsMap[td.id] = fmt.Errorf("Error#%d", td.value)
		} else {
			errorsMap[td.id] = nil
		}
	}

	return errorsMap, nil
}

type testBatchExecErrorsFn struct {
}

func (fn *testBatchExecErrorsFn) Execute(ctx context.Context, datum []DataIF) (map[string]error, error) {
	errorsMap := make(map[string]error)

	return errorsMap, errors.New("Test error")
}

func newTestJobs(numJobs int) []*job {
	js := make([]*job, numJobs)
	for i := 0; i < numJobs; i++ {
		js[i] = newJob(newTestData(i))
	}

	return js
}

func TestBatch_NewBatch(t *testing.T) {
	b := newBatch(0)
	if b == nil {
		t.Fatal("newBatch returned nil")
	}
}

func TestBatch_Add(t *testing.T) {
	b := newBatch(10)

	// create new jobs
	numJobs := rand.Intn(6) + 5
	js := newTestJobs(numJobs)

	// add one job
	b.add(js[0])

	// add two jobs
	b.add(js[1], js[2])

	// then add the rest
	b.add(js[3:]...)

	if len(b.jobsMap) != numJobs {
		t.Fatalf("jobsMap length invalid: wanted [%d], got [%d]", numJobs, len(b.jobsMap))
	}
	if len(b.datum) != numJobs {
		t.Fatalf("datum length invalid: wanted [%d], got [%d]", numJobs, len(b.datum))
	}

}

func TestBatch_Size(t *testing.T) {
	b := newBatch(10)

	if b.size() > 0 {
		t.Fatal("wrong initial size")
	}

	// create and add new jobs
	numJobs := rand.Intn(6) + 5
	js := newTestJobs(numJobs)
	b.add(js...)

	if b.size() != numJobs {
		t.Fatalf("size invalid: wanted [%d], got [%d]", numJobs, b.size())
	}
}

func TestBatch_UpdateSuccess(t *testing.T) {
	b := newBatch(10)

	numJobs := 2
	js := newTestJobs(numJobs)
	b.add(js...)

	b.updateSuccess("0", false)
	if b.successMap["0"] == nil {
		t.Fatal("successMap unexpected nil value")
	}
	if *b.successMap["0"] {
		t.Fatalf("successMap invalid: wanted [%t], got [%t]", false, *b.successMap["0"])
	}

	b.updateSuccess("1", true)
	if b.successMap["1"] == nil {
		t.Fatal("successMap unexpected nil value")
	}
	if !*b.successMap["1"] {
		t.Fatalf("successMap invalid: wanted [%t], got [%t]", true, *b.successMap["1"])
	}
}

func TestBatch_ExecuteSuccess(t *testing.T) {
	b := newBatch(10)

	numJobs := 10
	js := newTestJobs(numJobs)
	b.add(js...)

	fn := testBatchExecEvensFailFn{}
	err := b.execute(context.TODO(), fn.Execute)
	if err != nil {
		t.Fatal("unexpected error ", err)
	}

	for k, success := range b.successMap {
		if success == nil {
			t.Fatal("successMap unexpected nil value")
		}

		id, _ := strconv.Atoi(k)
		if id%2 == 0 {
			if *success {
				t.Fatalf("successMap invalid: wanted [%t], got [%t]", false, *success)
			}
		} else {
			if !*success {
				t.Fatalf("successMap invalid: wanted [%t], got [%t]", true, !*success)
			}
		}
	}
}

func TestBatch_ExecuteFailure(t *testing.T) {
	b := newBatch(10)

	numJobs := 10
	js := newTestJobs(numJobs)
	b.add(js...)

	fn := testBatchExecErrorsFn{}
	err := b.execute(context.TODO(), fn.Execute)

	if err == nil {
		t.Fatal("expected error but got nil")
	}
}

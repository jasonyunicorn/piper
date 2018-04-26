package piper

import (
	"context"
)

// BatchExecFn is a method signature which defines the expectations of a BatchExecutable Execute function
type BatchExecFn func(context.Context, []DataIF) (map[string]error, error)

// BatchExecutable is an interface which exposes the Execute method, which is the user-defined batch execution call
type BatchExecutable interface {
	Execute(context.Context, []DataIF) (map[string]error, error)
}

// batch is a struct which wraps all the data for a batch job along with the batch job meta information
type batch struct {
	datum      []DataIF         // slice of data
	jobsMap    map[string]*job  // map of Job IDs to Job
	successMap map[string]*bool // map of Job IDs to boolean indicating success/failure of Job
}

// newBatch creates a pointer to a batch
func newBatch(size int) *batch {
	return &batch{
		datum:      make([]DataIF, 0),
		jobsMap:    make(map[string]*job, size),
		successMap: make(map[string]*bool, size),
	}
}

// size returns the number of jobs in the batch
func (b *batch) size() int {
	return len(b.jobsMap)
}

// add appends jobs to the batch
func (b *batch) add(jobs ...*job) {
	for _, job := range jobs {
		b.jobsMap[job.id] = job
		b.datum = append(b.datum, job.data)
	}
}

// execute invokes the user-defined batch executable callback function and updates metadata about the batch job
func (b *batch) execute(ctx context.Context, fn BatchExecFn) error {
	size := b.size()
	if size > 0 {
		// Execute the batch function call and update batch metadata
		errorMap, err := fn(ctx, b.datum)
		if err != nil {
			return err
		}
		for k, v := range errorMap {
			if v == nil {
				b.updateSuccess(k, true)
			} else {
				b.updateSuccess(k, false)
				b.jobsMap[k].addError(v)
			}
		}
	}

	return nil
}

// updateSuccess updates the status of the successMap for a given job ID
func (b *batch) updateSuccess(id string, success bool) {
	b.successMap[id] = &success
}

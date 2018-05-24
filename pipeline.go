package piper

import (
	"context"
	"fmt"
	"sync"
)

// Pipeline is an object used for chaining multiple Processes together sequentially
type Pipeline struct {
	// required
	name      string     // Name of the pipeline
	processes []*Process // Processes that make up the pipeline arranged in order that they should be run

	// configurable
	wg *sync.WaitGroup // used to wait until data processing is complete

	// internal
	exec executable // mechanism for starting / stopping
}

// NewPipeline creates a pointer to a Pipeline
func NewPipeline(name string, processes []*Process, fns ...PipelineOptionFn) (*Pipeline, error) {
	if len(processes) < 2 {
		return nil, fmt.Errorf("Pipelines require at least 2 processes, got [%d] process(es)", len(processes))
	}
	p := &Pipeline{
		name:      name,
		processes: processes,
		wg:        &sync.WaitGroup{},
	}
	p.exec = newExec(p.startFn, p.stopFn)

	// Apply functional options
	for _, fn := range fns {
		fn(p)
	}

	return p, nil
}

// PipelineOptionFn is a method signature used for configuring the configurable fields of Pipeline
type PipelineOptionFn func(p *Pipeline)

func PipelineWithWaitGroup(wg *sync.WaitGroup) PipelineOptionFn {
	return func(p *Pipeline) {
		p.wg = wg
	}
}

// startFn defines the startup procedure for a Pipeline
func (p *Pipeline) startFn(ctx context.Context) {
	// Chain the processes together by adding the next Process's processData callback function
	// as an OnSuccessFn for the previous Process
	for i, process := range p.processes {
		if i > 0 {
			func(processPtr *Process) {
				p.processes[i-1].pushOnSuccessFns(func(data DataIF) []DataIF {
					processPtr.ProcessData(data)
					return []DataIF{}
				})
			}(process)
		}
	}

	for _, process := range p.processes {
		// Make all of the processes share the same WaitGroup
		process.setWaitGroup(p.wg)

		// Then start the all the process
		process.exec.start(ctx)
	}
}

// stopFn defines the shutdown procedure for a Pipeline
func (p *Pipeline) stopFn(ctx context.Context) {
	for _, process := range p.processes {
		process.exec.stop(ctx)
	}
}

// Start is used to trigger the Pipeline's startup sequence
func (p *Pipeline) Start(ctx context.Context) {
	p.exec.start(ctx)
}

// Stop is used to trigger the Pipeline's shutdown sequence
func (p *Pipeline) Stop(ctx context.Context) {
	p.exec.stop(ctx)
}

// ProcessData puts data on the queue for batch processing.  The processing is a synchronous
// operation, so the method returns as soon as the job is put on the queue, which should be
// almost instantly assuming the number of jobs in the queue is less than the queue depth.
func (p *Pipeline) ProcessData(data DataIF) {
	p.processes[0].ProcessData(data)
}

// ProcessDataAsync puts data on the queue for batch processing and waits for the job to finish
// before returning.  It only makes sense to use this method if there is one data point to process.
// To optimize performance when using this method, set the maxBatchSize to 1.
func (p *Pipeline) ProcessDataAsync(data DataIF) {
	p.ProcessData(data)
	p.wg.Wait()
}

// ProcessDatum puts all data on the queue for batch processing.  The process is a synchronous
// operation, so the method returns as soon as the jobs are put on the queue, which should be
// almost instantly assuming the number of jobs in the queue is less than the queue depth.
func (p *Pipeline) ProcessDatum(datum []DataIF) {
	p.processes[0].ProcessDatum(datum)
}

// ProcessDatumAsync puts all data on the queue for batch processing and waits until all data has
// been processed.
func (p *Pipeline) ProcessDatumAsync(datum []DataIF) {
	p.processes[0].ProcessDatumAsync(datum)
}

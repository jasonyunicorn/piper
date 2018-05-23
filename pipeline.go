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

	// internal
	exec executable      // mechanism for starting / stopping
	wg   *sync.WaitGroup // used to wait until ProcessData is complete
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

	//// Apply functional options
	//for _, fn := range fns {
	//	fn(p)
	//}

	return p, nil
}

// PipelineOptionFn is a method signature used for configuring the configurable fields of Pipeline
type PipelineOptionFn func(p *Pipeline)

// startFn defines the startup procedure for a Pipeline
func (p *Pipeline) startFn(ctx context.Context) {
	// Chain the processes together by adding the next Process's ProcessData callback function
	// as an OnSuccessFn for the previous Process
	for i, process := range p.processes {
		if i > 0 {
			func(processPtr *Process) {
				p.processes[i-1].pushOnSuccessFns(func(data DataIF) []DataIF {
					processPtr.processData(data)
					return []DataIF{}
				})
			}(process)
		}
	}

	for _, process := range p.processes {
		// Make all of the processes share the same WaitGroup
		process.wg = p.wg
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

// ProcessDatum puts all data on the queue for batch processing and waits until all data has been processed
func (p *Pipeline) ProcessDatum(datum []DataIF) {
	p.processes[0].ProcessDatum(datum)
}

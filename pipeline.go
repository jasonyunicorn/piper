package piper

import (
	"context"
	"fmt"
)

// Pipeline is an object used for chaining multiple Processes together sequentially
type Pipeline struct {
	// required
	name      string     // Name of the pipeline
	processes []*Process // Processes that make up the pipeline arranged in order that they should be run

	// internal
	exec executable
}

// NewPipeline creates a pointer to a Pipeline
func NewPipeline(name string, processes []*Process, fns ...PipelineOptionFn) (*Pipeline, error) {
	if len(processes) < 2 {
		return nil, fmt.Errorf("Pipelines require at least 2 processes, got [%d] process(es)", len(processes))
	}
	p := &Pipeline{
		name:      name,
		processes: processes,
	}
	p.exec = newExec(p.startFn, p.stopFn)

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
					processPtr.ProcessData(data)
					return []DataIF{}
				})
			}(process)
		}
	}

	// Then start the all the processes
	for _, process := range p.processes {
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

// ProcessData puts data on the first Process's queue for batch processing
func (p *Pipeline) ProcessData(data DataIF) {
	p.processes[0].ProcessData(data)
}

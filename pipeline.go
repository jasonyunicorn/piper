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
	onStartFn execFnType //

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
		onStartFn: func(ctx context.Context, wg *sync.WaitGroup) {
			defer wg.Done()
		},
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

// PipelineWithOnStartFn is an option function for configuring the Pipeline's onStartFn
func PipelineWithOnStartFn(onStartFn execFnType) PipelineOptionFn {
	return func(p *Pipeline) {
		p.onStartFn = onStartFn
	}
}

// startFn defines the startup procedure for a Pipeline
func (p *Pipeline) startFn(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

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
		wg.Add(1)
		go process.exec.start(ctx, wg)
	}

	wg.Add(1)
	go p.onStartFn(context.TODO(), wg)
}

// stopFn defines the shutdown procedure for a Pipeline
func (p *Pipeline) stopFn(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, process := range p.processes {
		wg.Add(1)
		go process.exec.stop(ctx, wg)
	}
}

// Start is used to trigger the Pipeline's startup sequence
func (p *Pipeline) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)
	p.exec.start(ctx, wg)
}

// Stop is used to trigger the Pipeline's shutdown sequence
func (p *Pipeline) Stop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)
	p.exec.stop(ctx, wg)
}

func (p *Pipeline) ProcessData(data DataIF) {
	p.processes[0].ProcessData(data)
}

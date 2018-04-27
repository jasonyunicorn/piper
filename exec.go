package piper

import (
	"context"
	"sync"
)

// execFnType
type execFnType func(context.Context, *sync.WaitGroup)

// executable is an interface which exposes the start and stop methods
type executable interface {
	start(context.Context, *sync.WaitGroup)
	stop(context.Context, *sync.WaitGroup)
}

// exec is a struct which implements the executable interface and is used for managing the startup and shutdown of processes
type exec struct {
	execMutex sync.Mutex // mutex used to prevent race conditions between start and stop
	startOnce sync.Once  // used to ensure that the startup function is called only once
	stopOnce  sync.Once  // used to ensure that the shutdown function is called only once
	startFn   execFnType // callback function called during startup
	stopFn    execFnType // callback function called during shutdown
}

// newExec creates a pointer to a exec given a startFn and a stopFn
func newExec(startFn, stopFn execFnType) *exec {
	return &exec{
		startFn: startFn,
		stopFn:  stopFn,
	}
}

// start triggers the startup sequence by calling startFn
func (e *exec) start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Lock the mutex to prevent race conditions with Stop
	e.execMutex.Lock()
	defer e.execMutex.Unlock()

	// Do the startup sequence once until the shutdown sequence resets
	e.startOnce.Do(func() {
		defer func() {
			// reset stopOnce so the shutdown sequence can happen again
			e.stopOnce = sync.Once{}
		}()
		wg.Add(1)
		e.startFn(ctx, wg)
	})
}

// stop triggers the shutdown sequence by calling stopFn
func (e *exec) stop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Lock the mutex to prevent race conditions with Start
	e.execMutex.Lock()
	defer e.execMutex.Unlock()

	// Do the shutdown sequence once until the startup sequence resets
	e.stopOnce.Do(func() {
		defer func() {
			// reset startOnce so the startup sequence can happen again
			e.startOnce = sync.Once{}
		}()
		wg.Add(1)
		e.stopFn(ctx, wg)
	})
}

package batch

import (
	"context"
	"sync"
)

// executable is an interface which exposes the start and stop methods
type executable interface {
	start(context.Context)
	stop(context.Context)
}

// exec is a struct which implements the executable interface and is used for managing the startup and shutdown of processes
type exec struct {
	execMutex sync.Mutex            // mutex used to prevent race conditions between start and stop
	startOnce sync.Once             // used to ensure that the startup function is called only once
	stopOnce  sync.Once             // used to ensure that the shutdown function is called only once
	startFn   func(context.Context) // callback function called during startup
	stopFn    func(context.Context) // callback function called during shutdown
}

// newExec creates a pointer to a exec given a startFn and a stopFn
func newExec(startFn, stopFn func(context.Context)) *exec {
	return &exec{
		startFn: startFn,
		stopFn:  stopFn,
	}
}

// start triggers the startup sequence by calling startFn
func (ss *exec) start(ctx context.Context) {
	// Lock the mutex to prevent race conditions with Stop
	ss.execMutex.Lock()
	defer ss.execMutex.Unlock()

	// Do the startup sequence once until the shutdown sequence resets
	ss.startOnce.Do(func() {
		defer func() {
			// reset stopOnce so the shutdown sequence can happen again
			ss.stopOnce = sync.Once{}
		}()
		ss.startFn(ctx)
	})
}

// stop triggers the shutdown sequence by calling stopFn
func (ss *exec) stop(ctx context.Context) {
	// Lock the mutex to prevent race conditions with Start
	ss.execMutex.Lock()
	defer ss.execMutex.Unlock()

	// Do the shutdown sequence once until the startup sequence resets
	ss.stopOnce.Do(func() {
		defer func() {
			// reset startOnce so the startup sequence can happen again
			ss.startOnce = sync.Once{}
		}()
		ss.stopFn(ctx)
	})
}

package piper

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// default number of concurrent workers processing batch jobs
	DEFAULT_CONCURRENCY int = 5

	// default maximum number of items to queue for the process
	DEFAULT_QUEUE_DEPTH int = 10000

	// default maximum duration to wait to fill a batch before processing what has been batched
	DEFAULT_BATCH_TIMEOUT time.Duration = time.Second

	// default maximum number of items to process in a batch
	DEFAULT_MAX_BATCH_SIZE int = 500

	// default maximum number of retries to attempt to before calling a failure callback function
	DEFAULT_MAX_RETRIES int = 10

	// default maximum frequency of batch function calls
	DEFAULT_RATE_LIMIT rate.Limit = rate.Inf
)

// ProcessFn is a method signature which defines the expectations of the OnSuccess and OnFailure callback functions
type ProcessFn func(DataIF) []DataIF

// Process is an object used for managing the execution of batch jobs amongst multiple concurrent workers
type Process struct {
	// required
	name      string          // name of the process
	batchExec BatchExecutable // batch function to call on batch jobs

	// configurable
	concurrency  int             // number of concurrent workers processing batch jobs
	queueDepth   int             // maximum number of items to queue
	batchTimeout time.Duration   // maximum duration to wait to fill a batch before processing what has been batched
	maxBatchSize int             // maximum number of items to process in a batch
	maxRetries   int             // maximum number of retries to attempt to before sending to failover queue
	rateLimit    rate.Limit      // maximum allowed frequency of batch function calls
	onSuccessFns []ProcessFn     // callback function that gets called when a job has been executed successfully
	onFailureFns []ProcessFn     // callback function that gets called when a job has exceeded the maximum number of retries
	wg           *sync.WaitGroup // used to wait until data processing is complete

	// internal
	exec    executable    // mechanism for starting / stopping
	jobsCh  chan *job     // channel for queued jobs to be processed
	workers []worker      // slice of workers used to track concurrent workers
	stopCh  chan struct{} // channel used to gracefully stop a process
}

// newDefaultProcess creates a pointer to a Process which contains default fields
func newDefaultProcess(name string, batchExec BatchExecutable) *Process {
	onSuccessFns := make([]ProcessFn, 0)
	onFailureFns := make([]ProcessFn, 0)
	return &Process{
		name:         name,
		batchExec:    batchExec,
		concurrency:  DEFAULT_CONCURRENCY,
		queueDepth:   DEFAULT_QUEUE_DEPTH,
		batchTimeout: DEFAULT_BATCH_TIMEOUT,
		maxBatchSize: DEFAULT_MAX_BATCH_SIZE,
		maxRetries:   DEFAULT_MAX_RETRIES,
		rateLimit:    DEFAULT_RATE_LIMIT,
		onSuccessFns: onSuccessFns,
		onFailureFns: onFailureFns,
		wg:           &sync.WaitGroup{},
	}
}

// NewProcess creates a pointer to a Process
func NewProcess(name string, batchExec BatchExecutable, fns ...ProcessOptionFn) *Process {
	p := newDefaultProcess(name, batchExec)
	p.exec = newExec(p.startFn, p.stopFn)

	// Apply functional options
	for _, fn := range fns {
		fn(p)
	}

	return p
}

// ProcessOptionFn is a method signature used for configuring the configurable fields of Process
type ProcessOptionFn func(p *Process)

// ProcessWithConcurrency is an option function for configuring the Process's concurrency
func ProcessWithConcurrency(concurrency int) ProcessOptionFn {
	return func(p *Process) {
		p.concurrency = concurrency
	}
}

// ProcessWithQueueDepth is an option function for configuring the Process's queueDepth
func ProcessWithQueueDepth(depth int) ProcessOptionFn {
	return func(p *Process) {
		p.queueDepth = depth
	}
}

// ProcessWithBatchTimeout is an option function for configuring the Process's batchTimeout
func ProcessWithBatchTimeout(timeout time.Duration) ProcessOptionFn {
	return func(p *Process) {
		p.batchTimeout = timeout
	}
}

// ProcessWithMaxBatchSize is an option function for configuring the Process's maxBatchSize
func ProcessWithMaxBatchSize(size int) ProcessOptionFn {
	return func(p *Process) {
		p.maxBatchSize = size
	}
}

// ProcessWithMaxRetries is an option function for configuring the Process's maxRetries
func ProcessWithMaxRetries(retries int) ProcessOptionFn {
	return func(p *Process) {
		p.maxRetries = retries
	}
}

// ProcessWithRateLimit is an option function for configuring the Process's rateLimit
func ProcessWithRateLimit(limit rate.Limit) ProcessOptionFn {
	return func(p *Process) {
		p.rateLimit = limit
	}
}

// ProcessWithOnSuccessFn is an option function for configuring the Process's onSuccessFn
func ProcessWithOnSuccessFns(fns ...ProcessFn) ProcessOptionFn {
	return func(p *Process) {
		p.onSuccessFns = fns
	}
}

// ProcessWithOnFailureFn is an option function for configuring the Process's onFailureFn
func ProcessWithOnFailureFns(fns ...ProcessFn) ProcessOptionFn {
	return func(p *Process) {
		p.onFailureFns = fns
	}
}

// ProcessWithWaitGroup is an option function for configuring the Process's onFailureFn
func ProcessWithWaitGroup(wg *sync.WaitGroup) ProcessOptionFn {
	return func(p *Process) {
		p.wg = wg
	}
}

// pushOnSuccessFn is a method used to add additional OnSuccessFn functions to a Process
func (p *Process) pushOnSuccessFns(fns ...ProcessFn) {
	p.onSuccessFns = append(p.onSuccessFns, fns...)
}

// pushOnFailureFns is a method used to add additional OnFailureFn functions to a Process
func (p *Process) pushOnFailureFns(fns ...ProcessFn) {
	p.onFailureFns = append(p.onFailureFns, fns...)
}

// applyFns function applies the slice of callback functions (recursively) to the datum
func (p *Process) applyFns(fns []ProcessFn, datum []DataIF) {
	for _, data := range datum {
		if len(fns) > 1 {
			p.applyFns(fns[1:], fns[0](data))
		} else {
			fns[0](data)
		}
	}
}

// startFn defines the startup procedure for a Process
func (p *Process) startFn(ctx context.Context) {
	// Initialize stuff
	limiter := rate.NewLimiter(p.rateLimit, 1)
	p.jobsCh = make(chan *job)
	p.stopCh = make(chan struct{})
	statusCh := make(chan *status)

	// Instantiate and start (concurrent) workers
	for i := 0; i < p.concurrency; i++ {
		w := newWorker(p.batchExec.Execute, statusCh)
		p.workers = append(p.workers, *w)
		w.exec.start(ctx)
	}

	go func() {
		// dispatch jobs to workers as necessary
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case status := <-statusCh:
				batch := newBatch(p.maxBatchSize)

				// Loop through the results and handle successes / failures
				var failures int
				if status.results != nil {
					for id, success := range status.results.successMap {
						job := status.results.jobsMap[id]
						if success != nil && *success {
							p.applyFns(p.onSuccessFns, []DataIF{job.data})
							p.wg.Done()
						} else if success != nil && !*success {
							failures++
							if job.retries < p.maxRetries {
								job.incrementRetry()
								batch.add(job)
							} else {
								p.applyFns(p.onFailureFns, []DataIF{job.data})
								p.wg.Done()
							}
						}
					}
				}

				// Fill the batch with new jobs off the queue
				// Or send what can be batched within the batch timeout period
				timeout := time.NewTimer(p.batchTimeout)
			batch:
				for batch.size() < p.maxBatchSize-failures {
					select {
					case <-ctx.Done():
						return
					case <-p.stopCh:
						return
					case <-timeout.C:
						break batch
					case job := <-p.jobsCh:
						batch.add(job)
						continue
					}
				}

				// Throttle the frequency of batch function calls
				if batch.size() > 0 {
					limiter.Wait(ctx)
				}

				// Send batch to the worker; if batch is empty, send anyways
				status.address <- batch
			}
		}
	}()
}

// stopFn defines the shutdown procedure for a Process
func (p *Process) stopFn(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case p.stopCh <- struct{}{}:
		// Stop Process before stopping workers
		for _, w := range p.workers {
			w.exec.stop(ctx)
		}
	}
}

// setWaitGroup updates the Process's internal WaitGroup to use the provided WaitGroup.  This is used for pipelines to share the same WaitGroup across all processes.
func (p *Process) setWaitGroup(wg *sync.WaitGroup) {
	p.wg = wg
}

// Start is used to trigger the Process's startup sequence
func (p *Process) Start(ctx context.Context) {
	p.exec.start(ctx)
}

// Start is used to trigger the Process's shutdown sequence
func (p *Process) Stop(ctx context.Context) {
	p.exec.stop(ctx)
}

// ProcessData puts data on the queue for batch processing.  The processing is a synchronous
// operation, so the method returns as soon as the job is put on the queue, which should be
// almost instantly assuming the number of jobs in the queue is less than the queue depth.
func (p *Process) ProcessData(data DataIF) {
	p.wg.Add(1)
	p.jobsCh <- newJob(data)
}

// ProcessDataAsync puts data on the queue for batch processing and waits for the job to finish
// before returning.  It only makes sense to use this method if there is one data point to process.
// To optimize performance when using this method, set the maxBatchSize to 1.
func (p *Process) ProcessDataAsync(data DataIF) {
	p.ProcessData(data)
	p.wg.Wait()
}

// ProcessDatum puts all data on the queue for batch processing.  The process is a synchronous
// operation, so the method returns as soon as the jobs are put on the queue, which should be
// almost instantly assuming the number of jobs in the queue is less than the queue depth.
func (p *Process) ProcessDatum(datum []DataIF) {
	defer p.wg.Done()

	p.wg.Add(1)
	for _, data := range datum {
		p.ProcessData(data)
	}
}

// ProcessDatumAsync puts all data on the queue for batch processing and waits until all data has
// been processed.
func (p *Process) ProcessDatumAsync(datum []DataIF) {
	p.ProcessDatum(datum)
	p.wg.Wait()
}

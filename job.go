package piper

// job is an internal struct which wraps the dataIF interface with metadata about the job
type job struct {
	id      string  // unique identifier to distinguish jobs
	retries int     // number of retry attempts
	errors  []error // array of errors encountered when processing this job
	data    DataIF  // data container for the contents of a single job
}

// newJob creates a pointer to a new job using the data's GetID() method to define the job ID
func newJob(data DataIF) *job {
	return &job{
		id:   data.GetID(),
		data: data,
	}
}

// incrementRetry increments the job's retry counter
func (j *job) incrementRetry() {
	j.retries++
}

// addError appends an error to the job's error metadata
func (j *job) addError(err error) {
	j.errors = append(j.errors, err)
}

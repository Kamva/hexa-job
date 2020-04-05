//--------------------------------
// Worker contains interfaces to
// define workers to process background
// jobs.
//--------------------------------
package hjob

import "github.com/Kamva/hexa"

type (

	// JobHandlerFunc is the handler of each job in the worker.
	JobHandlerFunc func(ctx hexa.Context, payload interface{}) error

	// Job is the new to push to the queue by Jobs interface
	Job struct {
		Name  string // required
		Queue string
		// Retry specify retry counts of the job.
		// 0: means that throw job away (and dont push to dead queue) on first fail.
		// -1: means that push job to the dead queue on first fail.
		Retry   int
		Payload interface{} // It can be any struct.
	}

	// Worker is the background jobs worker
	Worker interface {
		// Register handler for new job
		Register(name string, payloadInstance interface{}, handlerFunc JobHandlerFunc) error

		// Set worker concurrency
		Concurrency(c int) error

		// start process on some queues
		Process(queues ...string) error
	}

	// Jobs pushes jobs to process by worker.
	Jobs interface {
		// Push push job to the default queue
		Push(hexa.Context, *Job) error
	}
)

// NewJob returns new job instance
func NewJob(name string, payload interface{}) *Job {
	return NewJobWithQueue(name, "default", payload)
}

// NewJobWithQueue returns new job instance
func NewJobWithQueue(name string, queue string, p interface{}) *Job {
	return &Job{
		Name:    name,
		Queue:   queue,
		Retry:   4,
		Payload: p,
	}
}

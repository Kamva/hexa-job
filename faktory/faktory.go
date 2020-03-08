package hexafaktory

import (
	"errors"
	"github.com/Kamva/gutil"
	"github.com/Kamva/hexa"
	"github.com/Kamva/hexa-job"
	"github.com/Kamva/tracer"
	"github.com/contribsys/faktory/client"
	faktoryworker "github.com/contribsys/faktory_worker_go"
	"time"
)

type (
	// jobs is implementation of hexa Jobs using faktory
	jobs struct {
		p *client.Pool
	}

	// worker is implementation of hexa worker using faktory.
	worker struct {
		w  *faktoryworker.Manager
		uf hexa.UserFinder
		l  hexa.Logger
		t  hexa.Translator
	}
)

func (j *jobs) prepare(c hexa.Context, job *hjob.Job) *client.Job {

	if job.Queue == "" {
		job.Queue = "default"
	}

	ctxMap := c.ToMap()
	return &client.Job{
		Jid:       client.RandomJid(),
		Type:      job.Name,
		Queue:     job.Queue,
		Args:      []interface{}{ctxMap, gutil.StructToMap(job.Payload)},
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Retry:     job.Retry,
		// We don't using this custom data in any middleware, but just put it here :)
		Custom: ctxMap,
	}
}

func (j *jobs) Push(ctx hexa.Context, job *hjob.Job) error {
	if job == nil || job.Name == "" {
		return tracer.Trace(errors.New("job is not valid (enter job name please)"))
	}

	return j.p.With(func(conn *client.Client) error {
		return conn.Push(j.prepare(ctx, job))
	})
}

func (w *worker) handler(h hjob.JobHandlerFunc) faktoryworker.Perform {
	return func(ctx faktoryworker.Context, args ...interface{}) error {
		var payload hjob.Payload
		ctxMap := args[0].(map[string]interface{})
		err := gutil.MapToStruct(args[1].(map[string]interface{}), &payload)

		if err != nil {
			return tracer.Trace(err)
		}

		kCtx, err := hexa.CtxFromMap(ctxMap, w.uf, w.l, w.t)

		if err != nil {
			return tracer.Trace(err)
		}
		return h(kCtx, payload)
	}
}

func (w *worker) Register(name string, h hjob.JobHandlerFunc) error {
	w.w.Register(name, w.handler(h))
	return nil
}

func (w *worker) Concurrency(c int) error {
	w.w.Concurrency = c
	return nil
}

func (w *worker) Process(queues ...string) error {
	w.w.ProcessStrictPriorityQueues(queues...)
	// Run method does not return.
	w.w.Run()

	return nil
}

// NewFaktoryJobsDriver returns new instance of Jobs driver for the faktory
func NewFaktoryJobsDriver(p *client.Pool) hjob.Jobs {
	return &jobs{p}
}

// NewFaktoryWorkerDriver returns new instance of hexa Worker driver for the faktory
func NewFaktoryWorkerDriver(w *faktoryworker.Manager, uf hexa.UserFinder, l hexa.Logger, t hexa.Translator) hjob.Worker {
	return &worker{
		w:  w,
		uf: uf,
		l:  l,
		t:  t,
	}
}

var _ hjob.Jobs = &jobs{}
var _ hjob.Worker = &worker{}

package faktory

import (
	"errors"
	"github.com/Kamva/gutil"
	"github.com/Kamva/kitty"
	"github.com/Kamva/tracer"
	"github.com/contribsys/faktory/client"
	faktoryworker "github.com/contribsys/faktory_worker_go"
	"time"
)

type (
	// jobs is implementation of kitty Jobs using faktory
	jobs struct {
		p *client.Pool
	}

	// worker is implementation of kitty worker using faktory.
	worker struct {
		w  *faktoryworker.Manager
		uf kitty.UserFinder
		l  kitty.Logger
		t  kitty.Translator
	}
)

func (j *jobs) prepare(c kitty.Context, job *kitty.Job) *client.Job {

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

func (j *jobs) Push(ctx kitty.Context, job *kitty.Job) error {
	if job == nil || job.Name == "" {
		return tracer.Trace(errors.New("job is not valid (enter job name please)"))
	}

	return j.p.With(func(conn *client.Client) error {
		return conn.Push(j.prepare(ctx, job))
	})
}

func (w *worker) handler(h kitty.JobHandlerFunc) faktoryworker.Perform {
	return func(ctx faktoryworker.Context, args ...interface{}) error {

		var payload kitty.Payload
		ctxMap := args[0].(map[string]interface{})
		err := gutil.MapToStruct(args[1].(map[string]interface{}), &payload)

		if err != nil {
			return tracer.Trace(err)
		}

		kCtx, err := kitty.CtxFromMap(ctxMap, w.uf, w.l, w.t)

		if err != nil {
			return tracer.Trace(err)
		}
		return h(kCtx, payload)
	}
}

func (w *worker) Register(name string, h kitty.JobHandlerFunc) error {
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
func NewFaktoryJobsDriver(p *client.Pool) kitty.Jobs {
	return &jobs{p}
}

// NewFaktoryWorkerDriver returns new instance of kitty Worker driver for the faktory
func NewFaktoryWorkerDriver(w *faktoryworker.Manager, uf kitty.UserFinder, l kitty.Logger, t kitty.Translator) kitty.Worker {
	return &worker{
		w:  w,
		uf: uf,
		l:  l,
		t:  t,
	}
}

var _ kitty.Jobs = &jobs{}
var _ kitty.Worker = &worker{}

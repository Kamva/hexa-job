package hsyncq

import (
	"context"

	"github.com/hibiken/asynq"
	"github.com/kamva/hexa"
	hjob "github.com/kamva/hexa-job"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type jobs struct {
	cli         *asynq.Client
	p           hexa.ContextPropagator
	transformer Transformer
}

type worker struct {
	s           *asynq.Server
	mux         *asynq.ServeMux
	p           hexa.ContextPropagator
	transformer Transformer
}

func NewJobs(cli *asynq.Client, p hexa.ContextPropagator, t Transformer) hjob.Jobs {
	return &jobs{
		cli:         cli,
		p:           p,
		transformer: t,
	}
}

func NewWorker(s *asynq.Server, p hexa.ContextPropagator, t Transformer) hjob.Worker {
	return &worker{
		s:           s,
		mux:         asynq.NewServeMux(),
		p:           p,
		transformer: t,
	}
}

func (j *jobs) Push(c hexa.Context, job *hjob.Job) error {
	ctxData, err := j.p.Extract(c)
	if err != nil {
		return tracer.Trace(err)
	}
	b, err := j.transformer.BytesFromJob(ctxData, job.Payload)
	if err != nil {
		return tracer.Trace(err)
	}
	_, err = j.cli.Enqueue(asynq.NewTask(job.Name, b), asynq.Queue(job.Queue), asynq.MaxRetry(job.Retry), asynq.Timeout(job.Timeout))
	return tracer.Trace(err)
}

func (j *jobs) Shutdown(c context.Context) error {
	return tracer.Trace(j.cli.Close())
}

func (w *worker) Register(name string, handlerFunc hjob.JobHandlerFunc) error {
	w.mux.HandleFunc(name, func(ctx context.Context, task *asynq.Task) error {
		headers, p, err := w.transformer.PayloadFromBytes(task.Payload())
		if err != nil {
			return tracer.Trace(err)
		}

		ctx, err = w.p.Inject(headers, ctx)
		if err != nil {
			return tracer.Trace(err)
		}
		hexaContext, err := hexa.NewContextFromRawContext(ctx)
		if err != nil {
			return tracer.Trace(err)
		}
		err = tracer.Trace(handlerFunc(hexaContext, p))
		if err != nil {
			hexaContext.Logger().Error("error in handling queue task", hlog.ErrFields(err)...)
		}
		return err
	})

	return nil
}

func (w *worker) Run() error {
	return tracer.Trace(w.s.Run(w.mux))
}

func (w *worker) Shutdown(ctx context.Context) error {
	w.s.Shutdown()
	return nil
}

var _ hjob.Jobs = &jobs{}
var _ hexa.Shutdownable = &jobs{}
var _ hexa.Shutdownable = &worker{}

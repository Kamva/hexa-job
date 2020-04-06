package hexarobfig

import (
	"context"
	"github.com/Kamva/hexa"
	hjob "github.com/Kamva/hexa-job"
	"github.com/Kamva/tracer"
	"github.com/robfig/cron/v3"
)

type (
	// CronJobsOptions is the options that can provide on
	// create new cron job registerer to push cron jobs.
	CronJobsOptions struct {
		CtxGenerator CronJobCtxGenerator
		Cron         *cron.Cron
		Jobs         hjob.Jobs
		Worker       hjob.Worker
		Logger       hexa.Logger // optional
	}

	// CronJobCtxGenerator is a generator to generate new context to push as job's context.
	CronJobCtxGenerator func() hexa.Context

	// emptyPayload is just a empty payload as each job's payload.
	emptyPayload struct{}

	// cronJobs implements the hjob.CronJobs interface.
	cronJobs struct {
		ctxGenerator CronJobCtxGenerator
		logger       hexa.Logger
		cron         *cron.Cron
		jobs         hjob.Jobs
		worker       hjob.Worker
	}
)

func (c *cronJobs) Register(spec string, cJob *hjob.CronJob, handler hjob.CronJobHandlerFunc) error {
	if err := c.addCron(spec, cJob, handler); err != nil {
		return err
	}
	return c.registerJobHandler(cJob.Name, handler)
}

func (c *cronJobs) Start() error {
	c.cron.Start()
	return nil
}

func (c *cronJobs) Stop() (error, context.Context) {
	return nil, c.cron.Stop()
}

// addCron sets the cron job to push new job on each call to cron-job handler
func (c *cronJobs) addCron(spec string, cJob *hjob.CronJob, handler hjob.CronJobHandlerFunc) error {
	_, err := c.cron.AddFunc(spec, func() {
		err := c.jobs.Push(c.ctxGenerator(), c.job(cJob))
		if err != nil {
			c.reportFailedPush(cJob)
		}
	})

	return tracer.Trace(err)
}

// registerJobHandler sets the job handler.
func (c *cronJobs) registerJobHandler(jobName string, handler hjob.CronJobHandlerFunc) error {
	return c.worker.Register(jobName, emptyPayload{}, func(ctx hexa.Context, payload interface{}) error {
		return handler(ctx)
	})
}

// job convert Cron job to a regular job.
func (c *cronJobs) job(job *hjob.CronJob) *hjob.Job {
	return &hjob.Job{
		Name:    job.Name,
		Queue:   job.Queue,
		Retry:   job.Retry,
		Payload: emptyPayload{},
	}
}

func (c *cronJobs) reportFailedPush(job *hjob.CronJob) {
	if c.logger != nil {
		c.logger.WithFields("job_queue", job.Queue, "job_name", job.Name).
			Error("failed to push cron job to the jobs service.")
	}
}

// New returns new instance of the Cron Jobs
func New(options CronJobsOptions) hjob.CronJobs {
	return &cronJobs{
		ctxGenerator: options.CtxGenerator,
		logger:       options.Logger,
		cron:         options.Cron,
		jobs:         options.Jobs,
		worker:       options.Worker,
	}
}

// Assertion
var _ hjob.CronJobs = &cronJobs{}

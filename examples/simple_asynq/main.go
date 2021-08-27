package main

import (
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hjob "github.com/kamva/hexa-job"
	"github.com/kamva/hexa-job/hsynq"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type Payload struct {
	Name string `json:"name" mapstructure:"name"`
}

var logger = hlog.NewPrinterDriver(hlog.DebugLevel)
var translator = hexatranslator.NewEmptyDriver()
var propagator = hexa.NewContextPropagator(logger, translator)
var jobName = "example-job"

const redisAddr = "127.0.0.1:6379"

func main() {
	send()
	serve()
}

func send() {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	jobs := hsynq.NewJobs(client, propagator, hsynq.NewJsonTransformer())

	ctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: "test-cron-correlation-id",
		Locale:        "en",
		User:          hexa.NewGuest(),
		Logger:        logger,
		Translator:    translator,
	})

	err := jobs.Push(ctx, hjob.NewJob(jobName, Payload{Name: "mehran"}))
	gutil.PanicErr(err)
}

func serve() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			// Specify how many concurrent workers to use
			Concurrency: 10,
			// Optionally specify multiple queues with different priority.
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			// See the godoc for other configuration options
		},
	)

	worker := hsynq.NewWorker(srv, propagator, hsynq.NewJsonTransformer())

	gutil.PanicErr(worker.Register(jobName, sayHello))
	gutil.PanicErr(worker.Run())
}

func sayHello(context hexa.Context, payload hjob.Payload) error {
	fmt.Printf("%#v\n\n", context)
	var p Payload
	if err := payload.Decode(&p); err != nil {
		return tracer.Trace(err)
	}
	fmt.Printf("%#v\n\n", p)
	fmt.Printf("hello %s :) \n\n", p.Name)
	//return errors.New("I want to return errorrrr")
	return nil
}

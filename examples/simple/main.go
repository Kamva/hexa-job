package main

import (
	"context"
	"fmt"
	"os"

	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hjob "github.com/kamva/hexa-job"
	hexafaktory "github.com/kamva/hexa-job/faktory"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type Payload struct {
	Name string `json:"name" mapstructure:"name"`
}

func init() {
	_ = os.Setenv("FAKTORY_PROVIDER", "FAKTORY_URL")
	_ = os.Setenv("FAKTORY_URL", "tcp://localhost:7419")
}

var logger = hlog.NewPrinterDriver(hlog.DebugLevel)
var translator = hexatranslator.NewEmptyDriver()
var propagator = hexa.NewContextPropagator(logger, translator)
var jobName = "example-job"

func main() {
	send()
	serve()
}

func send() {
	client, err := faktory.NewPool(12)

	gutil.PanicErr(err)

	jobs := hexafaktory.NewFaktoryJobsDriver(client, propagator)

	ctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: "test-cron-correlation-id",
		Locale:        "en",
		User:          hexa.NewGuest(),
		BaseLogger:        logger,
		BaseTranslator:    translator,
	})

	err = jobs.Push(ctx, hjob.NewJob(jobName, Payload{Name: "mehran"}))
	gutil.PanicErr(err)
}

func serve() {
	w := worker.NewManager()
	server := hexafaktory.NewFaktoryWorkerDriver(w, propagator)

	gutil.PanicErr(server.Register(jobName, sayHello))
	gutil.PanicErr(server.Run())
}

func sayHello(context context.Context, payload hjob.Payload) error {
	fmt.Printf("%#v\n\n", context)
	fmt.Printf("%#v\n\n", payload)
	var p Payload
	if err := payload.Decode(&p); err != nil {
		return tracer.Trace(err)
	}
	fmt.Printf("hello %s :) \n\n", p.Name)
	return nil
}

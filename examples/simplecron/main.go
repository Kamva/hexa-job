package main

import (
	"fmt"
	"os"
	"time"

	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hjob "github.com/kamva/hexa-job"
	hexafaktory "github.com/kamva/hexa-job/faktory"
	hexarobfig "github.com/kamva/hexa-job/robfig"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/robfig/cron/v3"
)

func init() {
	_ = os.Setenv("FAKTORY_PROVIDER", "FAKTORY_URL")
	_ = os.Setenv("FAKTORY_URL", "tcp://localhost:7419")
}

var logger = hlog.NewPrinterDriver(hlog.DebugLevel)
var translator = hexatranslator.NewEmptyDriver()
var propagator = hexa.NewContextPropagator(logger, translator)
var cronJobName = "example-cron-job"

func main() {
	client, err := faktory.NewPool(12)
	w := worker.NewManager()
	gutil.PanicErr(err)

	jobs := hexafaktory.NewFaktoryJobsDriver(client, propagator)
	jobWorker := hexafaktory.NewFaktoryWorkerDriver(w, propagator)
	cronInstance := cron.New()

	cronJobs := hexarobfig.New(hexarobfig.CronJobsOptions{
		CtxGenerator: ctxGenerator,
		Cron:         cronInstance,
		Jobs:         jobs,
		Worker:       jobWorker,
		Logger:       logger,
	})

	gutil.PanicErr(cronJobs.Register("@every 3s", hjob.NewCronJob(cronJobName), sayHello))
	gutil.PanicErr(cronJobs.Run())
	gutil.PanicErr(jobWorker.Run())
}

func ctxGenerator() hexa.Context {
	return hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: "test-cron-correlation-id",
		Locale:        "en",
		User:          hexa.NewGuest(),
		Logger:        logger,
		Translator:    translator,
	})
}
func sayHello(ctx hexa.Context) error {
	fmt.Println("hello from cron job :) at:", time.Now())
	fmt.Println(ctx.CorrelationID())
	return nil
}

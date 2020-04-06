package main

import (
	"fmt"
	"github.com/Kamva/gutil"
	"github.com/Kamva/hexa"
	hjob "github.com/Kamva/hexa-job"
	hexafaktory "github.com/Kamva/hexa-job/faktory"
	hexarobfig "github.com/Kamva/hexa-job/robfig"
	"github.com/Kamva/hexa/db/mgmadapter"
	"github.com/Kamva/hexa/hexalogger"
	"github.com/Kamva/hexa/hexatranslator"
	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"github.com/robfig/cron/v3"
	"os"
	"time"
)

func init() {
	_ = os.Setenv("FAKTORY_PROVIDER", "FAKTORY_URL")
	_ = os.Setenv("FAKTORY_URL", "tcp://localhost:7419")
}

var logger = hexalogger.NewPrinterDriver()
var translator = hexatranslator.NewEmptyDriver()
var cExporter = hexa.NewCtxExporterImporter(hexa.NewUserExporterImporter(mgmadapter.EmptyID), logger, translator)
var cronJobName = "example-cron-job"

func main() {
	client, err := faktory.NewPool(12)
	w := worker.NewManager()
	gutil.PanicErr(err)

	jobs := hexafaktory.NewFaktoryJobsDriver(client, cExporter)
	jobWorker := hexafaktory.NewFaktoryWorkerDriver(w, cExporter)
	cronInstance := cron.New()

	cronJobs := hexarobfig.New(hexarobfig.CronJobsOptions{
		CtxGenerator: ctxGenerator,
		Cron:         cronInstance,
		Jobs:         jobs,
		Worker:       jobWorker,
		Logger:       logger,
	})

	gutil.PanicErr(cronJobs.Register("@every 3s", hjob.NewCronJob(cronJobName), sayHello))
	gutil.PanicErr(cronJobs.Start())
	gutil.PanicErr(jobWorker.Process("default"))
}

func ctxGenerator() hexa.Context {
	return hexa.NewCtx(nil, "test-cron-correlation-id", "en", hexa.NewGuest(), logger, translator)
}

func sayHello(ctx hexa.Context) error {
	fmt.Println("hello from cron job :) at:", time.Now())
	fmt.Println(ctx.CorrelationID())
	return nil
}

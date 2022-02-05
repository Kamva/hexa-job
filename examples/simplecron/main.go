package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hjob "github.com/kamva/hexa-job"
	hexarobfig "github.com/kamva/hexa-job/robfig"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/robfig/cron/v3"
)

var logger = hlog.NewPrinterDriver(hlog.DebugLevel)
var translator = hexatranslator.NewEmptyDriver()
var cronJobName = "example-cron-job"

func main() {

	cronInstance := cron.New()

	cronJobs := hexarobfig.New(ctxGenerator, cronInstance)

	gutil.PanicErr(cronJobs.Register("@every 3s", hjob.NewCronJob(cronJobName), sayHello))
	gutil.PanicErr(cronJobs.Run())
}

func ctxGenerator(c context.Context) hexa.Context {
	return hexa.NewContext(c, hexa.ContextParams{
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

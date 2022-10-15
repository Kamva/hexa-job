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
	done, err := cronJobs.Run()
	gutil.PanicErr(err)
	gutil.PanicErr(<-done)
}

func ctxGenerator(c context.Context) context.Context {
	return hexa.NewContext(c, hexa.ContextParams{
		CorrelationId:  "test-cron-correlation-id",
		Locale:         "en",
		User:           hexa.NewGuest(),
		BaseLogger:     logger,
		BaseTranslator: translator,
	})
}

func sayHello(ctx context.Context) error {
	fmt.Println("hello from cron job :) at:", time.Now())
	fmt.Println(hexa.CtxCorrelationId(ctx))
	return nil
}

package main

import (
	"fmt"
	"github.com/Kamva/gutil"
	"github.com/Kamva/hexa"
	hjob "github.com/Kamva/hexa-job"
	hexafaktory "github.com/Kamva/hexa-job/faktory"
	"github.com/Kamva/hexa/db/mgmadapter"
	"github.com/Kamva/hexa/hexalogger"
	"github.com/Kamva/hexa/hexatranslator"
	faktory "github.com/contribsys/faktory/client"
	worker "github.com/contribsys/faktory_worker_go"
	"os"
)

type Payload struct {
	Name string `json:"name" mapstructure:"name"`
}

func init() {
	_ = os.Setenv("FAKTORY_PROVIDER", "FAKTORY_URL")
	_ = os.Setenv("FAKTORY_URL", "tcp://localhost:7419")
}

var logger = hexalogger.NewPrinterDriver()
var translator = hexatranslator.NewEmptyDriver()
var cExporter = hexa.NewCtxExporterImporter(hexa.NewUserExporterImporter(mgmadapter.EmptyID), logger, translator)
var jobName = "example-job"

func main() {
	send()
	serve()
}

func send() {
	client, err := faktory.NewPool(12)

	gutil.PanicErr(err)

	jobs := hexafaktory.NewFaktoryJobsDriver(client, cExporter)

	ctx := hexa.NewCtx(nil, "test-correlation-id", "en", hexa.NewGuest(), logger, translator)

	err = jobs.Push(ctx, hjob.NewJob(jobName, Payload{Name: "mehran"}))
	gutil.PanicErr(err)
}

func serve() {
	w := worker.NewManager()
	server := hexafaktory.NewFaktoryWorkerDriver(w, cExporter)

	gutil.PanicErr(server.Register(jobName, Payload{}, sayHello))
	gutil.PanicErr(server.Process("default"))
}

func sayHello(context hexa.Context, payload interface{}) error {
	fmt.Printf("%#v\n\n", context)
	fmt.Printf("%#v\n\n", payload)
	var p = payload.(*Payload)
	fmt.Printf("hello %s :) \n\n", p.Name)
	return nil
}

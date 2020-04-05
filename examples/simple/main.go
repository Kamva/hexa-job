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

type PayloadData struct {
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

	payload := hjob.Payload{
		Header: hexa.Map{"foo-header": "bar"},
		Data:   gutil.StructToMap(PayloadData{Name: "mehran"}),
	}

	err = jobs.Push(ctx, hjob.NewJob(jobName, payload))
	gutil.PanicErr(err)
}

func serve() {
	w := worker.NewManager()
	server := hexafaktory.NewFaktoryWorkerDriver(w, cExporter)

	gutil.PanicErr(server.Register(jobName, sayHello))
	gutil.PanicErr(server.Process("default"))
}

func sayHello(context hexa.Context, payload hjob.Payload) error {
	fmt.Printf("%#v\n\n", context)
	fmt.Printf("%#v\n\n", payload)
	var p PayloadData
	gutil.PanicErr(gutil.MapToStruct(payload.Data, &p))
	fmt.Printf("hello %s :) \n\n", p.Name)
	return nil
}

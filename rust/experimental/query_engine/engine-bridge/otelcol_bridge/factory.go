package otelcol_bridge
/*
#cgo LDFLAGS: -L. -lengine_bridge -lm
#include "capi/engine_bridge.h"
*/
import "C"

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("otelcol_bridge"),
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return nil
}

func createLogsProcessor(
	ctx context.Context,
    set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs) (processor.Logs, error) {

	return processorhelper.NewLogs(
		ctx,
		set,
		cfg,
		nextConsumer,
		createLogsHandler,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func createLogsHandler(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	C.init_query_engine(C.CString("Log | filter event_id == 1")) // Initialize the query engine with a query

	req := plogotlp.NewExportRequestFromLogs(ld)
	fmt.Printf("Processing logs hello xyz2...%T\n", req)
	buf, _ := req.MarshalProto()
	fmt.Printf("Processing logs hello xyz3... %d in %T\n", len(buf), buf)
	fmt.Println("MarshalProto", buf)
	C.process((*C.char)(C.CBytes(buf)), C.size_t(len(buf)))
	return ld, nil
}
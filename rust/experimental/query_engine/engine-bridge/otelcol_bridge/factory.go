package otelcol_bridge
/*
#cgo LDFLAGS: -L. -lengine_bridge -lm
#include "capi/engine_bridge.h"
*/
import "C"

import (
	"context"
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
	C.init_query_engine(C.CString("Log | filter event_id == 1")) // Initialize the query engine with a query
	return &Config{
		// Initialize any default configuration fields here.
	}
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
		logProcessorHandler,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}

func logProcessorHandler(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	req := plogotlp.NewExportRequestFromLogs(ld)
	buf, _ := req.MarshalProto()
	C.process((*C.char)(C.CBytes(buf)), C.size_t(len(buf)))

	err := req.UnmarshalProto(buf)
	if err != nil {
		return ld, err
	}

	new_ld := req.Logs()
	return new_ld, nil
}
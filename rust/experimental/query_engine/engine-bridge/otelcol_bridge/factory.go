package otelcol_bridge

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
	req := plogotlp.NewExportRequestFromLogs(ld)
	fmt.Printf("Processing logs hello xyz2...%T\n", req)
	buf, _ := req.MarshalProto()
	fmt.Printf("Processing logs hello xyz3... %d in %T\n", len(buf), buf)
	fmt.Println("MarshalProto", buf)
	return ld, nil
}
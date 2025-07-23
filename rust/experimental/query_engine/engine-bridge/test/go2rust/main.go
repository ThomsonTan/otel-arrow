package main

/*
#cgo LDFLAGS: -L. -lengine_bridge -lm
#include "engine_bridge.h"
*/
import "C"

import logs_v1 "go.opentelemetry.io/proto/otlp/logs/v1"
import common_v1 "go.opentelemetry.io/proto/otlp/common/v1"
import "google.golang.org/protobuf/proto"

func init_scope_logs() *logs_v1.ScopeLogs {
	scope_logs := &logs_v1.ScopeLogs{
		LogRecords: []*logs_v1.LogRecord{
			{
				TimeUnixNano: 1234567890,
				SeverityNumber: logs_v1.SeverityNumber_SEVERITY_NUMBER_INFO,
				SeverityText: "Info",
				Body: &common_v1.AnyValue{
					Value: &common_v1.AnyValue_StringValue{
						StringValue: "This is a log message",
					},
				},
				Attributes: []*common_v1.KeyValue{
					{	
						Key: "event_id",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_IntValue{
								IntValue: 1,
							},
						},
					},
					{
						Key: "key1",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_StringValue{
								StringValue: "value1",
							},
						},
					},
				},
			},
			{
				TimeUnixNano: 1234567891,
				SeverityNumber: logs_v1.SeverityNumber_SEVERITY_NUMBER_ERROR,
				SeverityText: "Error",
				Body: &common_v1.AnyValue{
					Value: &common_v1.AnyValue_StringValue{
						StringValue: "This is a log error",
					},
				},
				Attributes: []*common_v1.KeyValue{
					{	
						Key: "event_id",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_IntValue{
								IntValue: 2,
							},
						},
					},
					{
						Key: "key2",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_StringValue{
								StringValue: "value2",
							},
						},
					},
				},
			},
		},
	}
	return scope_logs
}

func main() {
	C.init_query_engine(C.CString("Log | filter event_id == 1")) // Initialize the query engine with a query

	scope_logs := init_scope_logs() // Initialize the ScopeLogs structure
	data, _ := proto.Marshal(scope_logs) // Marshal the request to protobuf format
	C.process((*C.char)(C.CBytes(data)), C.size_t(len(data)))
}
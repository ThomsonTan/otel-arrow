package main

/*
#cgo LDFLAGS: -L. -lengine_api
#include "engine_api.h"
*/
import "C"

// import "go.opentelemetry.io/proto/otlp/collector/logs/v1"
// import "go.opentelemetry.io/proto/otlp/logs/v1"
import "go.opentelemetry.io/collector/pdata/plog"
// import "go.opentelemetry.io/collector/pdata/internal"
// import "example.com/go2rust/proto/otlp/collector/logs/v1"
import logs_v1 "go.opentelemetry.io/proto/otlp/logs/v1"
import common_v1 "go.opentelemetry.io/proto/otlp/common/v1"
import "google.golang.org/protobuf/proto"
import "reflect"

func main() {
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
						Key: "key1",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_StringValue{
								StringValue: "value1",
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
					{
						Key: "key3",
						Value: &common_v1.AnyValue{
							Value: &common_v1.AnyValue_StringValue{
								StringValue: "value3",
							},
						},
					},
				},
			},
		},
	}
	println("Request type:", reflect.TypeOf(scope_logs).String())
	data, _ := proto.Marshal(scope_logs) // Marshal the request to protobuf format
	println("Data length:", len(data))
	println("Data type", reflect.TypeOf(data).String())

	C.init_query_engine(C.CString("table name | key1, key2, key3 | filter key1 = 'value1' and key2 = 'value2'"))
	C.process((*C.char)(C.CBytes(data)), C.size_t(len(data)))
}
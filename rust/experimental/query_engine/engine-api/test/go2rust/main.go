package main

/*
#cgo LDFLAGS: -L. -lengine_api
#include "engine_api.h"
*/
import "C"

import "go.opentelemetry.io/collector/pdata/plog"
import "google.golang.org/protobuf/proto"

func main() {
	logs := plog.NewLogs()
	pb := logs.ResourceLogs()
	data, err := proto.Marshal(pb)
	result := C.init_query_engine()
	println("Result of init_query_engine:", result)
	println("Logs count:", logs.ResourceLogs().Len())
}
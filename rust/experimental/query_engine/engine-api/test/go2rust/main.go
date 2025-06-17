package main

/*
#cgo LDFLAGS: -L. -lengine_api
#include "engine_api.h"
*/
import "C"

func main() {
	result := C.init_query_engine()
	println("Result of init_query_engine:", result)
}
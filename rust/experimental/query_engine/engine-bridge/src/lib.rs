use data_engine_recordset::{
    data::*, data_expressions::*, logical_expressions::*, primitives::*, value_expressions::*, *,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs, LogRecord};
use opentelemetry_proto::tonic::common::v1::{any_value};
use prost::Message;


use std::sync::Mutex;
use once_cell::sync::Lazy;

pub mod common;

// initialize the global state to an empty instance
static GLOBAL_STATE: Lazy<Mutex<String>> = Lazy::new(|| {
    Mutex::new(String::new())
});

#[unsafe(no_mangle)]
pub extern "C" fn init_query_engine() -> i32 {
    let mut global = GLOBAL_STATE.lock().unwrap();
    *global = String::from("Initialized Query Engine");

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn process(buf: *mut u8, len: usize) -> i32 {
    match process_internal(buf, len) {
        Some(result) => result,
        None => -1,
    }
}

fn process_internal(buf: *mut u8, len: usize) -> Option<i32> {
    if buf.is_null() || len == 0 {
        return None;
    }

    let bytes = unsafe { std::slice::from_raw_parts(buf, len) };
    let mut batch = common::TestLogRecordBatch::new();
    match ExportLogsServiceRequest::decode(bytes) {
        Ok(export_log_service_request) => {
            export_log_service_request.resource_logs.iter().for_each(|resource_log| {
                println!("Resource Logs: {:?}", resource_log);

                resource_log.scope_logs.iter().for_each(|scope_log| {
                    // Iterate over scope_log.log_records
                    scope_log.log_records.iter().for_each(|log_record| {
                        println!("Log Record: {:?}", log_record);

                        let mut log_record1 = common::TestLogRecord::new();
                        log_record1.set_attribute("event_id", AnyValue::new_long_value(1));

                        if let Some(s) = log_record.body.as_ref().and_then(|av| {
                            match &av.value {
                                Some(any_value::Value::StringValue(string_value)) => Some(string_value.clone()),
                                _ => None
                            }
                        }) {
                            log_record1.set_body(AnyValue::new_string_value(s.as_ref()));
                        }

                        // Iterate over log_record.attributes
                        log_record.attributes.iter().for_each(|kv| {
                            if let Some(any_value) = &kv.value {
                                if let Some(any_value::Value::StringValue(string_value)) = &any_value.value {
                                    log_record1.set_attribute(kv.key.as_ref(), AnyValue::new_string_value(string_value.as_str()));
                                } else if let Some(any_value::Value::IntValue(int_value)) = &any_value.value {
                                    log_record1.set_attribute(kv.key.as_ref(), AnyValue::new_long_value(*int_value));
                                }
                            }
                        });
                        batch.add_log_record(log_record1);
                    });
                });
            });
        }
        Err(_err) => { }
    }

    let mut pipeline = PipelineExpression::new();
    pipeline.add_data_expression(DiscardDataExpression::new_with_predicate(
        EqualToLogicalExpression::new(
            ResolveValueExpression::new("event_id").ok()?,
            StaticValueExpression::new(AnyValue::new_long_value(2)),
        ),
    ));
    let data_engine = common::create_data_engine().ok()?;
    let results = data_engine.process_complete_batch(&pipeline, &mut batch).ok()?;

    println!("Included record count: {:?}", results.get_included_record_count());
    println!("Dropped record count: {:?}", results.get_dropped_record_count());
    None
}
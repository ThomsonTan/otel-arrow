use data_engine_recordset::{
    data_expressions::*, logical_expressions::*, primitives::*, value_expressions::*, *,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use opentelemetry_proto::tonic::logs::v1::ScopeLogs;

use std::sync::Mutex;

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
pub extern "C" fn process(buf: *const u8, len: usize) -> i32 {
    if buf.is_null() || len == 0 {
        return -1;
    }
    let bytes = unsafe { std::slice::from_raw_parts(buf, len) };
    match ScopeLogs::decode(bytes) {
        Ok(scopeLogs) => {
            scopeLogs.log_records.iter().for_each(|log_record| {
                println!("Log Record: {:?}", log_record);
            });
            let mut pipeline = PipelineExpression::new();

            pipeline.add_data_expression(DiscardDataExpression::new_with_predicate(
                EqualToLogicalExpression::new(
                    ResolveValueExpression::new("event_id"),
                    StaticValueExpression::new(AnyValue::new_long_value(1)),
                ),
            ));

            let data_engine = common::create_data_engine();
            // let results = data_engine.process_complete_batch(&pipeline, &mut scopeLogs.log_records);
            0
        }
        Err(_err) => {
            -1
        }
    }
}
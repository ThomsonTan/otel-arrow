use data_engine_recordset::{
    data::*, data_expressions::*, logical_expressions::*, primitives::*, value_expressions::*, *,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use opentelemetry_proto::tonic::logs::v1::{ScopeLogs, LogRecord};

use std::sync::Mutex;
use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
struct EngineLogRecordBatch(pub ScopeLogs);

impl DataRecordBatch<LogRecord> for EngineLogRecordBatch {
    fn drain<S: DataEngineState, F>(&mut self, state: &mut S, action: F) -> Result<(), Error>
    where
        F: Fn(&mut S, LogRecord) -> Result<(), Error>,
    {
        // Drain log_records by value, passing each to the closure
        // let log_records = std::mem::take(&mut self.log_records); // Faster than Vec::drain for all
        // for record in log_records {
        //     action(state, record)?; // Early exit on error
        // }
        Ok(())
    }
}

pub fn create_data_engine() -> Result<DataEngine, Error> {
    let mut data_engine = DataEngine::new();

    // data_engine.register::<TestResource>()?;
    // data_engine.register::<TestInstrumentationScope>()?;
    // data_engine.register::<TestLogRecord>()?;

    Ok(data_engine)
}

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

            // pipeline.add_data_expression(DiscardDataExpression::new_with_predicate(
            //     EqualToLogicalExpression::new(
            //         ResolveValueExpression::new("event_id"),
            //         StaticValueExpression::new(AnyValue::new_long_value(1)),
            //     ),
            // ));

            let data_engine = create_data_engine();
            // let results = data_engine.process_complete_batch(&pipeline, &mut scopeLogs.log_records);
            0
        }
        Err(_err) => {
            -1
        }
    }
}
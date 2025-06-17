use data_engine_recordset::{
    data_expressions::*, logical_expressions::*, primitives::*, value_expressions::*, *,
};

#[unsafe(no_mangle)]
pub extern "C" fn init_query_engine() -> i32 {
    let mut pipeline = PipelineExpression::new();

    // pipeline.add_data_expression(DiscardDataExpression::new_with_predicate(
    //     EqualToLogicalExpression::new(
    //         ResolveValueExpression::new("event_id"),
    //         StaticValueExpression::new(AnyValue::new_long_value(1)),
    //     ),
    // ));

    0
}
use serde_json::json;
use lapin::types::{AMQPValue, AMQPType};


pub fn string_to_header(val: &str) -> AMQPValue {
    let j = json!(val);
    return AMQPValue::try_from(&j, AMQPType::LongString).unwrap();
}
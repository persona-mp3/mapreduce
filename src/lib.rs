// Okay so we're going to start with the Map and reduce function
// before implementing rpcs

// A worker runs a Map(k, v) and a Reduce(k, Vec<T>)
// fn Map (k, v) -> Vec<KV>
//  where k-> input_file_name, v -> file_contents
pub fn hello_lib() {
    println!("Hello mapreduce impl");
}

#[derive(Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub fn worker_map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    // TODO: Not sure if we need to check if it's valid utf8 when getting the src
    let mut key_values: Vec<KeyValue> = vec![];
    for word in contents.split_ascii_whitespace() {
        key_values.push(KeyValue {
            key: String::from(word),
            value: String::from("1"),
        });
    }
    key_values
}

pub fn worker_reduce(_key: &str, values: Vec<String>) -> String {
    values.len().to_string()
}

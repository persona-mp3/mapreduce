mod coordinator;
mod worker;
// Source: http://nil.csail.mit.edu/6.5840/2025/labs/lab-mr.html

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The final state of this project should make use of RPC's for communication
    // between workers and the coordinator node
    {
        let _rpc_response = worker::stub_rpc_request_task();
    }

    let current_tasks = vec![
        coordinator::Task::new(String::from("tasks/adventure-of-sherlock-holmes.txt")),
        coordinator::Task::new(String::from("tasks/the_hemingway.txt")),
        coordinator::Task::new(String::from("tasks/caravan.txt")),
        coordinator::Task::new(String::from("tasks/poem.txt")),
    ];

    let _ = coordinator::coordinator(current_tasks, custom_map, custom_reduce);
    Ok(())
}

fn custom_map(key: &String, value: &str) -> Vec<worker::MKeyValue> {
    println!("[map]: {}", key);

    let mut cleaned_content: String = String::with_capacity(value.len());

    for ch in value.chars() {
        if ch.is_whitespace() || ch.is_alphabetic() {
            let us = format!("{}", ch);
            cleaned_content += &us;
        }
    }

    let value = cleaned_content.to_ascii_lowercase();

    let words = value.split_ascii_whitespace();
    let mut kv_pairs = vec![];
    for word in words {
        kv_pairs.push(worker::MKeyValue {
            key: String::from(word),
            value: String::from("1"),
        });
    }

    kv_pairs
}

fn custom_reduce(key: String, values: Vec<String>) -> String {
    let mut curr_count: u32 = 0;
    for val in values {
        let s = val.parse::<u32>().unwrap_or_default();
        curr_count += s;
    }

    // Matching the format provided by the exercise
    format!("{}: {}\n", key, curr_count)
}

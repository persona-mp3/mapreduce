#![allow(dead_code, unused)]
mod worker;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This is a stub, that we'll refactor to RPC's while been
    // multithreaded. This is supposed to simulate a worker
    // asking the co-ordinator main for a task, and when they
    // also report they're done w a task. But for now, we're asking for
    // a specific task
    let task_file_path = get_task("tasks/the_hemingway.txt", false);
    {
        let rpc_response = worker::ask_task();
    }

    worker::worker(&task_file_path, custom_map, custom_reduce)?;
    Ok(())
}

#[derive(PartialEq, Debug)]
enum State {
    Idle,
    InProgress,
    Done,
}

#[derive(Debug)]
struct Task {
    file_name: String,
    state: State,
}

fn get_task(task_name: &str, ask: bool) -> String {
    println!("[CO-ORDINATOR] Getting task");
    let mut current_tasks = vec![
        Task {
            file_name: String::from("tasks/the_hemingway.txt"),
            state: State::Idle,
        },
        Task {
            file_name: String::from("tasks/carvan.txt"),
            state: State::Idle,
        },
        Task {
            file_name: String::from("tasks/poem.txt"),
            state: State::Idle,
        },
    ];

    for mut task in &mut current_tasks {
        if task.file_name == task_name && ask && task.state == State::InProgress {
            task.state = State::Done;
            break;
        }
    }

    for mut task in &mut current_tasks {
        if task.state == State::Idle {
            task.state = State::InProgress;
            println!("[CO-ORDINATOR] Assigning task: {:?}", task);
            return task.file_name.clone();
        }
    }

    String::from("")
}

// NOTE: Key is src_file
fn custom_map(key: &String, value: &str) -> Vec<worker::MKeyValue> {
    // let content =
    //     fs::read_to_string(key).expect(" [USER_MAP] Could not open source_file for key provided ");
    //
    println!("[USER_MAP]: Starting implementation");
    let value = value.to_ascii_lowercase();
    let mut words = value.split_ascii_whitespace();
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
    // Technically, you could just get the length of `values`, but
    // if these values were different, then, this would be simply wrong
    let mut curr_count: u32 = 0;
    for val in values {
        // this is corrupted data, but since this is user impl, we might not bother?
        // but since data is corrupted, might as well quit the whole job
        let s = val.parse::<u32>().unwrap_or_default();
        curr_count += s;
    }

    format!("{} -> {}", key, curr_count)
}

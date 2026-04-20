use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::{fs, thread};

#[derive(Debug, PartialEq)]
pub struct MKeyValue {
    pub key: String,
    pub value: String,
}

pub type MapFn = fn(key: &String, value: &str) -> Vec<MKeyValue>;
pub type ReduceFn = fn(key: String, values: Vec<String>) -> String;

pub fn stub_rpc_request_task() -> String {
    String::new()
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct HashCounter {
    pub key: String,
    pub values: Vec<String>,
}

// Currenttly: Right now, threads can just send the value of a computation but we can't really tell
// for which task, so we'll want to wrap this in a reponse Stuct, kinda in the same way. At the same
// time, we might want to extend it for the work to request for more work that it can carry out.
// This wat the coordinator knows how to update each task as Failed, Done, Idle, InProgess
#[derive(Debug)]
pub struct Response {
    pub id: String,
    pub value: Option<Vec<MKeyValue>>,
}

#[derive(Debug)]
pub struct WorkerInstruction {
    /// Send channel the worker uses to communicate with the coordinator
    pub send_response: Sender<Response>,

    /// The worker will look into this file path to get the input.
    ///
    /// At the moment, it must be utf-8 encoded, otherwise the worker errors
    pub file_path: String,

    /// Map implementation of the user
    pub map_fn: MapFn,

    /// Reduce implementation of the user
    pub reduce_fn: ReduceFn,
}

pub const FILE_PREFIX: &str = "mr-out";

/// Worker function takes the file path of where the  work exists, extracts the contents and feeds
/// it into the `map_fn` and then into the `reduce_fn`. The list of key-value pairs returned are
/// from the result of the `map_fn`. The final output after the MapReduce is written to a
/// destination file.
pub fn worker(
    task_file_path: &String,
    map_fn: MapFn,
    reduce_fn: ReduceFn,
) -> Result<Vec<MKeyValue>, Box<dyn std::error::Error>> {
    let content = match fs::read_to_string(task_file_path) {
        Ok(v) => v,
        Err(err) => {
            let err_info = format!(
                "Could not open task_file: {}
                Reason: {err}",
                task_file_path
            );
            return Err(err_info.into());
        }
    };

    let mut list_kv_pairs = map_fn(task_file_path, &content);

    list_kv_pairs.sort_by_key(|kv| kv.key.clone());
    let mut tally: BTreeMap<String, HashCounter> = BTreeMap::new();
    for kv in list_kv_pairs.iter() {
        if !tally.contains_key(&kv.key) {
            tally.insert(
                kv.key.clone(),
                HashCounter {
                    key: kv.key.clone(),
                    values: vec![kv.value.clone()],
                },
            );

        } else {
            match tally.get_mut(&kv.key) {
                Some(hash_counter) => {
                    hash_counter.values.push(kv.value.clone());
                }
                _ => (),
            };
        }
    }

    let mut full_content = String::from("");
    for (k, v) in tally {
        let out = reduce_fn(k, v.values);
        full_content.push_str(out.as_str());
    }

    // TODO: Refactor: this is fragile and shoulld consider using os-seperators
    // but I think Rust handles that automatically for us
    let file_name = match task_file_path.find("/") {
        Some(index) => task_file_path.clone().split_off(index + 1),
        None => {
            println!("Couldnt find path seperator");
            return Err("Couldnt find path seperator".into());
        }
    };

    let dest_file_name = format!("{FILE_PREFIX}-{file_name}");
    match write_to_dest(&full_content, &dest_file_name) {
        Ok(_) => (),
        Err(err) => {
            return Err(err.into());
        }
    };

    println!("[worker] done with {task_file_path}");
    Ok(list_kv_pairs)
}

/// Spawns a thread and provides the instruction to the worker function. After success or failure
/// the `send channel` is dropped. This function panics on send. The handler is returned for the
/// caller or coordinator to join to their main thread
pub fn thread_worker(instruction: WorkerInstruction) -> JoinHandle<()> {
    let handle = thread::spawn(move || {
        let result = worker(
            &instruction.file_path,
            instruction.map_fn,
            instruction.reduce_fn,
        );

        let _ = match result {
            Ok(success) => {
                instruction
                    .send_response
                    .send(Response {
                        id: instruction.file_path,
                        value: Some(success),
                    })
                    .expect("[worker] could not send. orphaned. Coordinator has been closed");
                drop(instruction.send_response);
            }
            Err(err) => {
                println!("[worker-error]: {err}",);
                instruction
                    .send_response
                    .send(Response {
                        id: instruction.file_path,
                        value: None,
                    })
                    .expect("[worker] could not send. orphaned. Coordinator has been closed");
                drop(instruction.send_response);
                return;
            }
        };
    });

    handle
}

/// Workers write the output of their reduce function to RESULT_DIR/<PREFIX_NAME>-<filename>
pub const RESULT_DIR: &str = "results";

/// Writes output to destination file by creating it in the `RESULT_DIR`'s directory. If the file
/// already exists, the file is truncated and written to
fn write_to_dest(content: &str, dest: &str) -> Result<(), Box<dyn std::error::Error>> {
    let full_path = PathBuf::from(RESULT_DIR).join(dest);
    let mut handler = match fs::File::options().create(true).write(true).open(full_path) {
        Ok(fh) => fh,
        Err(err) => {
            let err_msg = format!(
                "Could not create {dest}. 
                Reason: {err}"
            );
            return Err(err_msg.into());
        }
    };

    let _bytes_written = match handler.write(content.as_bytes()) {
        Ok(n) => n,
        Err(err) => {
            let err_msg = format!(
                "
                Could not write to {dest}
                Reason: {err}
                "
            );

            return Err(err_msg.into());
        }
    };

    Ok(())
}

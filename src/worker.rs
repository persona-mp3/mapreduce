use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::{fs, thread};

#[derive(Debug)]
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
struct HashCounter {
    key: String,
    values: Vec<String>,
}

#[derive(Debug)]
pub struct WorkerInstruction {
    pub send_ch: Sender<Option<Vec<MKeyValue>>>,
    pub file_path: String,
    pub map_fn: MapFn,
    pub reduce_fn: ReduceFn,
}

const FILE_PREFIX: &str = "mr";

/// Worker function
pub fn worker(
    task_file_path: &String,
    map_fn: MapFn,
    reduce_fn: ReduceFn,
) -> Result<Vec<MKeyValue>, Box<dyn std::error::Error>> {
    println!("[WORKER] Woker starting");
    let content = match fs::read_to_string(task_file_path) {
        Ok(v) => v,
        Err(err) => {
            let err_info = format!(
                "[WORKER] could not open task_file: {}
                Reason: {err}",
                task_file_path
            );
            return Err(err_info.into());
        }
    };

    let list_kv_pairs = map_fn(task_file_path, &content);

    let mut tally: HashMap<String, HashCounter> = HashMap::with_capacity(list_kv_pairs.len());
    for kv in list_kv_pairs.iter() {
        if !tally.contains_key(&kv.key) {
            tally.insert(
                kv.key.clone(),
                HashCounter {
                    key: kv.key.clone(),
                    values: vec![kv.value.clone()],
                },
            );

            continue;
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
                    .send_ch
                    .send(Some(success))
                    .expect("[worker] could not send. orphaned. Coordinator has been closed");
                drop(instruction.send_ch);
            }
            Err(err) => {
                println!("[worker-error]: {err}",);
                instruction
                    .send_ch
                    .send(None)
                    .expect("[worker] could not send. orphaned. Coordinator has been closed");
                drop(instruction.send_ch);
                return;
            }
        };
    });

    handle
}

const RESULT_DIR: &str = "results";

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

    println!("Bytes written to {dest}: {_bytes_written}");
    Ok(())
}

use super::{Task, Uuid};
use chrono::Utc;
use rand::{thread_rng, Rng};
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

async fn run_foo_task(task: &Task) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("Foo {}", task.id.unwrap());
    Ok(())
}

async fn run_bar_task() -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::get("https://www.whattimeisitrightnow.com/").await?;

    println!("{}", resp.status());
    Ok(())
}

async fn run_baz_task() -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = thread_rng();
    let rand_numb = rng.gen_range(0..=343);

    println!("Baz {}", rand_numb);
    Ok(())
}

async fn set_status(
    task: Task,
    state: &str,
    db: Arc<DB>,
) -> Result<(), Box<dyn std::error::Error>> {
    db.put(
        serde_json::to_string(&task.id).unwrap(),
        serde_json::to_string(&Task {
            id: task.id,
            task_type: task.task_type,
            execution_time: task.execution_time,
            state: Some(state.to_string()),
        })
        .unwrap(),
    )
    .unwrap();

    Ok(())
}

pub async fn run_worker(db: Arc<DB>) {
    loop {
        let mut result: HashMap<Uuid, Task> = HashMap::new();

        for task in db.iterator(rocksdb::IteratorMode::Start) {
            let (key, val) = task.unwrap();
            result.insert(
                serde_json::from_str(String::from_utf8(key.to_vec()).unwrap().as_str()).unwrap(),
                serde_json::from_str(String::from_utf8(val.to_vec()).unwrap().as_str()).unwrap(),
            );
        }

        for (_, val) in result.iter() {
            let now = Utc::now().timestamp();
            if val.state == Some("new".to_string()) && now >= val.execution_time {
                set_status(val.clone(), "running", db.clone())
                    .await
                    .unwrap();

                match val.task_type.as_str() {
                    "Foo" => run_foo_task(val).await.unwrap(),
                    "Bar" => run_bar_task().await.unwrap(),
                    "Baz" => run_baz_task().await.unwrap(),
                    _ => (),
                }

                set_status(val.clone(), "done", db.clone()).await.unwrap();
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

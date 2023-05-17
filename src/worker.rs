use super::{State, Task, TaskType, Uuid};
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
    let client = reqwest::Client::builder()
        .user_agent("task-api v0.0.1")
        .build()?;
    let resp = client
        .get("https://www.whattimeisitrightnow.com/")
        .send()
        .await?;

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
    state: State,
    db: Arc<DB>,
) -> Result<(), Box<dyn std::error::Error>> {
    db.put(
        serde_json::to_string(&task.id).unwrap(),
        serde_json::to_string(&Task {
            id: task.id,
            task_type: task.task_type,
            execution_time: task.execution_time,
            state: Some(state),
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
            if val.state == Some(State::New) && now >= val.execution_time {
                set_status(val.clone(), State::Running, db.clone())
                    .await
                    .unwrap();

                match val.task_type {
                    TaskType::Foo => run_foo_task(val).await.unwrap(),
                    TaskType::Bar => run_bar_task().await.unwrap(),
                    TaskType::Baz => run_baz_task().await.unwrap(),
                }

                set_status(val.clone(), State::Done, db.clone())
                    .await
                    .unwrap();
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

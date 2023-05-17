mod worker;

use chrono::{Duration, Utc};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use warp::{http, Filter};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
enum TaskType {
    Foo,
    Bar,
    Baz,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
enum State {
    New,
    Running,
    Done,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Id {
    id: Uuid,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Task {
    id: Option<Uuid>,
    task_type: TaskType,
    execution_time: i64,
    state: Option<State>,
}

async fn add_task(task: Task, db: Arc<DB>) -> Result<impl warp::Reply, warp::Rejection> {
    let id = Uuid::new_v4();

    db.put(
        serde_json::to_string(&id).unwrap(),
        serde_json::to_string(&Task {
            id: Some(id),
            task_type: task.task_type,
            execution_time: (Utc::now() + Duration::seconds(task.execution_time)).timestamp(),
            state: Some(State::New),
        })
        .unwrap(),
    )
    .unwrap();

    Ok(warp::reply::with_status(
        format!("Added task ({}) to queue", id),
        http::StatusCode::CREATED,
    ))
}

async fn delete_task(id: Id, db: Arc<DB>) -> Result<impl warp::Reply, warp::Rejection> {
    db.delete(serde_json::to_string(&id.id).unwrap()).unwrap();

    Ok(warp::reply::with_status(
        "Removed task from queue",
        http::StatusCode::OK,
    ))
}

async fn get_task(id: String, db: Arc<DB>) -> Result<impl warp::Reply, warp::Rejection> {
    let uuid = Uuid::parse_str(&id).unwrap();
    let task: Task = serde_json::from_str(
        String::from_utf8(
            db.get(serde_json::to_string(&uuid).unwrap())
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .as_str(),
    )
    .unwrap();

    Ok(warp::reply::json(&task))
}

async fn get_task_list(db: Arc<DB>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut result: HashMap<Uuid, Task> = HashMap::new();

    for task in db.iterator(rocksdb::IteratorMode::Start) {
        let (key, val) = task.unwrap();
        result.insert(
            serde_json::from_str(String::from_utf8(key.to_vec()).unwrap().as_str()).unwrap(),
            serde_json::from_str(String::from_utf8(val.to_vec()).unwrap().as_str()).unwrap(),
        );
    }

    Ok(warp::reply::json(&result))
}

fn post_json() -> impl Filter<Extract = (Task,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn delete_json() -> impl Filter<Extract = (Id,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

#[tokio::main]
async fn main() {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    let db = Arc::new(DB::open(&opts, "./tasks.db").unwrap());
    let cloned_db = db.clone();

    tokio::spawn(async move {
        worker::run_worker(cloned_db).await;
    });

    let store_filter = warp::any().map(move || db.clone());

    let add_tasks = warp::post()
        .and(warp::path("tasks"))
        .and(warp::path::end())
        .and(post_json())
        .and(store_filter.clone())
        .and_then(add_task);

    let get_tasks = warp::get()
        .and(warp::path("tasks"))
        .and(warp::path::end())
        .and(store_filter.clone())
        .and_then(get_task_list);

    let get_task = warp::get()
        .and(warp::path!("tasks" / String))
        .and(warp::path::end())
        .and(store_filter.clone())
        .and_then(get_task);

    let delete_tasks = warp::delete()
        .and(warp::path("tasks"))
        .and(warp::path::end())
        .and(delete_json())
        .and(store_filter.clone())
        .and_then(delete_task);

    let routes = add_tasks.or(get_tasks).or(get_task).or(delete_tasks);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

use std::sync::Arc;
use storage::storage::Storage;
use storage::{BgTask, BgTaskHandler, DataType};

// This test ensures:
// - All tasks are sent successfully (no panic)
// - The worker can process tasks and exit cleanly (no deadlock)
#[cfg(not(miri))]
#[tokio::test]
async fn test_bg_task_worker_concurrent() {
    let mut storage = Storage::new(1, 0);
    let (handler, receiver) = BgTaskHandler::new();
    storage.bg_task_handler = Some(Arc::new(handler));
    let storage = Arc::new(storage);

    let worker_storage = Arc::clone(&storage);
    let worker_handle = tokio::spawn(async move {
        Storage::bg_task_worker(worker_storage, receiver).await;
    });

    let handler = storage.bg_task_handler.as_ref().unwrap().clone();
    let mut handles = vec![];
    for _ in 0..10 {
        let handler_clone = handler.clone();
        handles.push(tokio::spawn(async move {
            handler_clone
                .send(BgTask::CleanAll {
                    dtype: DataType::All,
                })
                .await
                .unwrap();
        }));
        let handler_clone = handler.clone();
        handles.push(tokio::spawn(async move {
            handler_clone
                .send(BgTask::CompactRange {
                    dtype: DataType::All,
                    start: "a".into(),
                    end: "z".into(),
                })
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    handler.send(BgTask::Shutdown).await.unwrap();
    worker_handle.await.unwrap();
}

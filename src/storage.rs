use std::time::SystemTime;
use tokio::sync::mpsc;

pub(crate) struct Entry {
    pub(crate) val: String,
    pub(crate) exp: Option<SystemTime>,
}

#[tokio::test]
async fn test_channel() {
    let (tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        for i in 0..50 {
            if let Err(_) = tx.send(i).await {
                println!("receiver dropped");
                return;
            }
        }
    });

    while let Some(i) = rx.recv().await {
        println!("got = {}", i);
    }
}

use std::time::SystemTime;

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) val: String,
    pub(crate) exp: Option<SystemTime>,
}

#[tokio::test]
async fn test_channel() {
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        for i in 0..50 {
            if tx.send(i).await.is_err() {
                println!("receiver dropped");
                return;
            }
        }
    });

    while let Some(i) = rx.recv().await {
        println!("got = {}", i);
    }
}

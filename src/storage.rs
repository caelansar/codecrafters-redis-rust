use crate::protocol::RESP;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio::sync::{broadcast, Notify};
use tokio::time;
use tokio::time::Instant;

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// The key-value data. We are not trying to do anything fancy so a
    /// `std::collections::HashMap` works fine.
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `mini-redis` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    stream: HashMap<String, BTreeMap<String, Vec<(String, String)>>>,

    block_stream: HashMap<String, Vec<Sender<RESP>>>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeSet` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique key (`String`) is used to
    /// break these ties.
    expirations: BTreeSet<(Instant, String)>,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawns a
    /// background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                stream: HashMap::new(),
                block_stream: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start the background task.
        println!("start purge_expired_tasks");
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub(crate) fn get_all_block(&self, key: impl AsRef<str>) -> Vec<Sender<RESP>> {
        let mut state = self.shared.state.lock().unwrap();
        match state.block_stream.get_mut(key.as_ref()) {
            Some(senders) => mem::take(senders),
            None => vec![],
        }
    }

    pub(crate) fn block_stream(&self, key: String) -> Receiver<RESP> {
        use std::collections::hash_map::Entry;

        let (tx, rx) = channel();
        let mut state = self.shared.state.lock().unwrap();
        match state.block_stream.entry(key) {
            Entry::Occupied(mut e) => {
                e.get_mut().push(tx);
            }
            Entry::Vacant(e) => {
                e.insert(vec![tx]);
            }
        }
        rx
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key. This may be
    /// due to never having assigned a value to the key or a previously assigned
    /// value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire the lock, get the entry and clone the value.
        //
        // Because data is stored using `Bytes`, a clone here is a shallow
        // clone. Data is not copied.
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub(crate) fn set_stream(
        &self,
        key: impl ToString,
        id: impl ToString,
        data: Vec<(String, String)>,
    ) -> anyhow::Result<()> {
        use std::collections::hash_map::Entry;

        if id.to_string() == "0-0" {
            anyhow::bail!("ERR The ID specified in XADD must be greater than 0-0")
        }

        let mut state = self.shared.state.lock().unwrap();
        let entry = state.stream.entry(key.to_string());
        match entry {
            Entry::Occupied(mut e) => {
                let set = e.get();

                let last = set.iter().last();
                match last {
                    None => {
                        let mut set = BTreeMap::new();
                        set.insert(id.to_string(), data);
                        e.insert(set);
                    }
                    Some(x) if *x.0 < id.to_string() => {
                        let entry = e.get_mut();
                        entry.insert(id.to_string(), data);
                    }
                    Some(_) => {
                        anyhow::bail!("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                    }
                }
            }
            Entry::Vacant(e) => {
                let mut set = BTreeMap::new();
                set.insert(id.to_string(), data);
                e.insert(set);
            }
        }
        Ok(())
    }

    pub(crate) fn get_stream(&self, key: &str) -> Option<BTreeMap<String, Vec<(String, String)>>> {
        let state = self.shared.state.lock().unwrap();
        state.stream.get(key).cloned()
    }

    pub(crate) fn keys(&self) -> Vec<RESP> {
        let state = self.shared.state.lock().unwrap();
        state
            .entries
            .keys()
            .map(|x| RESP::BulkString(Bytes::from(x.clone())))
            .collect::<Vec<RESP>>()
    }

    /// Set the value associated with a key along with an optional expiration
    /// Duration.
    ///
    /// If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified, so it can update its state.
        //
        // Whether the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // Only notify the worker task if the newly inserted expiration is the
            // **next** key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // Insert the entry into the `HashMap`.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // If there was a value previously associated with the key **and** it
        // had an expiration time. The associated entry in the `expirations` map
        // must also be removed. This avoids leaking data.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // Track the expiration. If we insert before remove that will cause bug
        // when current `(when, key)` equals prev `(when, key)`. Remove then insert
        // can avoid this.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);

        if notify {
            // Finally, only notify the background task if it needs to update
            // its state to reflect a new expiration.
            self.shared.background_task.notify_one();
        }
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down. This is called by the
    /// `DbShutdown`s `Drop` implementation.
    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            println!("remove expired key: {}", key);
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }

    println!("Purge background task shut down")
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

#[tokio::test]
async fn test_db() {
    let db = Db::new();

    db.set("k".to_string(), Bytes::from("v"), None);
    assert_eq!(Some(Bytes::from("v")), db.get("k"));

    db.set(
        "k".to_string(),
        Bytes::from("v1"),
        Some(Duration::from_secs(1)),
    );
    time::sleep(Duration::from_millis(1010)).await;
    assert_eq!(None, db.get("k"));

    let mut recv = db.subscribe("k".to_string());

    let db1 = db.clone();
    tokio::spawn(async move {
        let n = db1.publish("k", Bytes::from("v"));
        assert_eq!(1, n);
    });

    assert_eq!(Ok(Bytes::from("v")), recv.recv().await);

    let data = Vec::new();

    db.set_stream("s", "1-1", data.clone()).unwrap();
    db.set_stream("s", "1-2", data.clone()).unwrap();

    assert!(
        db.set_stream("s", "1-2", data.clone()).is_err(),
        "Passing in an ID lower than should result in an error"
    );
    assert!(db.set_stream("s", "0-0", data).is_err());
}

#[tokio::test]
async fn test_db_stream() {
    let db = Db::new();

    let rx = db.block_stream("stream".to_string());

    let dbc = db.clone();
    tokio::spawn(async move {
        let senders = dbc.get_all_block("stream");
        assert_eq!(1, senders.len());

        senders
            .into_iter()
            .for_each(|sender| sender.send(RESP::SimpleString("1".to_string())).unwrap());
    });

    assert_eq!(Ok(RESP::SimpleString("1".to_string())), rx.await);

    let senders = db.get_all_block("stream");
    assert_eq!(0, senders.len());
}

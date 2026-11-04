//! FIFO admission for transaction validation and commit.
use super::MAX_ACTIVE_SNAPSHOTS;
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// Cooperative cancellation is checked while waiting; scheduling and lock
// delays mean this polling interval is not a bounded-latency promise.
const CANCELLATION_POLL: Duration = Duration::from_millis(10);

struct Waiter {
    wake: Condvar,
    owner: std::thread::ThreadId,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Arc<Waiter>>,
    failed: bool,
}

pub(crate) struct TransactionQueue {
    state: Mutex<State>,
}

impl TransactionQueue {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn queued_for_test(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .queue
            .len()
    }

    pub(crate) fn enter(&self) -> io::Result<Turn<'_>> {
        self.enter_until(None)
    }

    pub(crate) fn enter_until(&self, deadline: Option<Instant>) -> io::Result<Turn<'_>> {
        self.enter_controlled(deadline, None)
    }

    pub(crate) fn enter_controlled(
        &self,
        deadline: Option<Instant>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> io::Result<Turn<'_>> {
        let waiter = Arc::new(Waiter {
            wake: Condvar::new(),
            owner: std::thread::current().id(),
        });
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.failed = true;
                wake_all(&state);
                return Err(io::Error::other("transaction admission queue poisoned"));
            }
        };
        if state.failed {
            return Err(io::Error::other("transaction admission queue failed"));
        }
        if state.queue.iter().any(|entry| entry.owner == waiter.owner) {
            return Err(io::Error::other(
                "nested transaction admission on the same thread is unsupported",
            ));
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "transaction admission expired",
            ));
        }
        crate::cancellation::check(cancellation)?;
        if state.queue.len() >= MAX_ACTIVE_SNAPSHOTS {
            return Err(io::Error::other("transaction admission queue full"));
        }
        state
            .queue
            .try_reserve(1)
            .map_err(|_| io::Error::other("transaction admission allocation failed"))?;
        state.queue.push_back(Arc::clone(&waiter));
        loop {
            if state.failed {
                state.queue.retain(|entry| !Arc::ptr_eq(entry, &waiter));
                wake_all(&state);
                return Err(io::Error::other("transaction admission queue failed"));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let was_first = state
                    .queue
                    .front()
                    .is_some_and(|entry| Arc::ptr_eq(entry, &waiter));
                state.queue.retain(|entry| !Arc::ptr_eq(entry, &waiter));
                if was_first {
                    if let Some(next) = state.queue.front() {
                        next.wake.notify_one();
                    }
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "transaction admission expired",
                ));
            }
            if let Err(error) = crate::cancellation::check(cancellation) {
                let was_first = state
                    .queue
                    .front()
                    .is_some_and(|entry| Arc::ptr_eq(entry, &waiter));
                state.queue.retain(|entry| !Arc::ptr_eq(entry, &waiter));
                if was_first {
                    if let Some(next) = state.queue.front() {
                        next.wake.notify_one();
                    }
                }
                return Err(error);
            }
            if state
                .queue
                .front()
                .is_some_and(|entry| Arc::ptr_eq(entry, &waiter))
            {
                return Ok(Turn {
                    queue: self,
                    waiter,
                });
            }
            let timeout =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            let timeout = if cancellation.is_some() {
                Some(timeout.map_or(CANCELLATION_POLL, |remaining| {
                    remaining.min(CANCELLATION_POLL)
                }))
            } else {
                timeout
            };
            let waited = if let Some(timeout) = timeout {
                waiter
                    .wake
                    .wait_timeout(state, timeout)
                    .map(|(state, _)| state)
                    .map_err(|poison| poison.into_inner().0)
            } else {
                waiter
                    .wake
                    .wait(state)
                    .map_err(|poison| poison.into_inner())
            };
            state = match waited {
                Ok(state) => state,
                Err(mut state) => {
                    state.failed = true;
                    state.queue.retain(|entry| !Arc::ptr_eq(entry, &waiter));
                    wake_all(&state);
                    return Err(io::Error::other("transaction admission queue poisoned"));
                }
            };
        }
    }
}

fn wake_all(state: &State) {
    for waiter in &state.queue {
        waiter.wake.notify_one();
    }
}

pub(crate) struct Turn<'a> {
    queue: &'a TransactionQueue,
    waiter: Arc<Waiter>,
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        let (mut state, poisoned) = match self.queue.state.lock() {
            Ok(state) => (state, false),
            Err(poison) => (poison.into_inner(), true),
        };
        if poisoned {
            state.failed = true;
        }
        state
            .queue
            .retain(|entry| !Arc::ptr_eq(entry, &self.waiter));
        if state.failed {
            wake_all(&state);
        } else if let Some(next) = state.queue.front() {
            next.wake.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn wait_for_count(queue: &TransactionQueue, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if queue
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .queue
                .len()
                == count
            {
                return;
            }
            assert!(Instant::now() < deadline, "transaction did not enter queue");
            thread::yield_now();
        }
    }

    #[test]
    fn waiting_transactions_enter_in_fifo_order() {
        let queue = Arc::new(TransactionQueue::new());
        let first = queue.enter().unwrap();
        let (order_tx, order_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for index in 0..5 {
            let waiting = Arc::clone(&queue);
            let order_tx = order_tx.clone();
            workers.push(thread::spawn(move || {
                let _turn = waiting.enter().unwrap();
                order_tx.send(index).unwrap();
            }));
            wait_for_count(&queue, index + 2);
        }
        drop(first);
        for expected in 0..5 {
            assert_eq!(
                order_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
                expected
            );
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(queue.state.lock().unwrap().queue.is_empty());
    }

    #[test]
    fn panicking_turn_releases_the_next_waiter() {
        let queue = TransactionQueue::new();
        let result = std::panic::catch_unwind(|| {
            let _turn = queue.enter().unwrap();
            panic!("injected commit panic");
        });
        assert!(result.is_err());
        drop(queue.enter().unwrap());
        assert!(queue.state.lock().unwrap().queue.is_empty());
    }

    #[test]
    fn poisoned_queue_wakes_waiters_and_fails_closed() {
        let queue = Arc::new(TransactionQueue::new());
        let first = queue.enter().unwrap();
        let waiting = Arc::clone(&queue);
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let failed = waiting.enter().is_err();
            done_tx.send(failed).unwrap();
        });
        wait_for_count(&queue, 2);
        let poisoned = std::panic::catch_unwind(|| {
            let _state = queue.state.lock().unwrap();
            panic!("injected queue panic");
        });
        assert!(poisoned.is_err());
        drop(first);
        assert!(done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
        worker.join().unwrap();
        assert!(queue.enter().is_err());
    }

    #[test]
    fn queue_rejects_excess_waiters_without_disturbing_existing_turns() {
        let queue = Arc::new(TransactionQueue::new());
        let first = queue.enter().unwrap();
        let mut workers = Vec::new();
        for index in 1..MAX_ACTIVE_SNAPSHOTS {
            let waiting = Arc::clone(&queue);
            workers.push(thread::spawn(move || drop(waiting.enter().unwrap())));
            wait_for_count(&queue, index + 1);
        }
        let extra = Arc::clone(&queue);
        assert!(
            thread::spawn(move || extra.enter().is_err())
                .join()
                .unwrap()
        );
        drop(first);
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(queue.state.lock().unwrap().queue.is_empty());
    }

    #[test]
    fn nested_admission_on_the_same_thread_returns_an_error() {
        let queue = TransactionQueue::new();
        let first = queue.enter().unwrap();
        let error = match queue.enter() {
            Ok(_) => panic!("nested queue entry should fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Other);
        drop(first);
        drop(queue.enter().unwrap());
    }

    #[test]
    fn fifo_is_opt_in_and_does_not_block_other_transaction_modes() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::Store::open(directory.path()).unwrap());
        let first = store.transaction_queue.enter().unwrap();
        store.transaction(|tx| tx.put(b"default", b"ok")).unwrap();
        store
            .transaction_with_read_set(|tx| tx.put(b"readset", b"ok"))
            .unwrap();
        let waiting = Arc::clone(&store);
        let worker = thread::spawn(move || {
            waiting.transaction_with_read_set_fifo(|tx| tx.put(b"fifo", b"ok"))
        });
        wait_for_count(&store.transaction_queue, 2);
        drop(first);
        worker.join().unwrap().unwrap();
        assert_eq!(store.get(b"fifo").unwrap().as_deref(), Some(&b"ok"[..]));
    }

    #[test]
    fn fifo_deadline_removes_a_waiter_without_blocking_followers() {
        use crate::{TimedTransactionError, TransactionMode};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::Store::open(directory.path()).unwrap());
        let first = store.transaction_queue.enter().unwrap();
        let waiting = Arc::clone(&store);
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            let result = waiting.transaction_until(deadline, TransactionMode::ReadSetFifo, |tx| {
                tx.put(b"expired", b"must-not-commit")
            });
            done_tx.send(result).unwrap();
        });
        wait_for_count(&store.transaction_queue, 2);
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(TimedTransactionError::DeadlineExceeded)
        ));
        worker.join().unwrap();
        assert_eq!(store.transaction_queue.queued_for_test(), 1);
        assert!(store.get(b"expired").unwrap().is_none());
        let following = Arc::clone(&store);
        let follower = thread::spawn(move || {
            following.transaction_with_read_set_fifo(|tx| tx.put(b"following", b"ok"))
        });
        wait_for_count(&store.transaction_queue, 2);
        drop(first);
        follower.join().unwrap().unwrap();
        assert_eq!(
            store.get(b"following").unwrap().as_deref(),
            Some(&b"ok"[..])
        );
    }
}

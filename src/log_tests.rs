//! Fault injection at the log-sync and witness-update boundaries.
use super::*;
use crate::witness::{self, WitnessIo};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    LogSync,
    WitnessRead,
    WitnessWriteBefore,
    WitnessWriteAfter,
    WitnessSync,
}

struct Shared {
    events: Mutex<Vec<&'static str>>,
    witness: Mutex<Vec<u8>>,
    fault: Fault,
}
struct LogHandle(Arc<Shared>);
struct WitnessHandle(Arc<Shared>);

impl LogIo for LogHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(128)
    }
    fn read_exact_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<()> {
        unreachable!()
    }
    fn append_all(&self, _bytes: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("log_sync");
        if self.0.fault == Fault::LogSync {
            return Err(io::Error::other("log sync failure"));
        }
        Ok(())
    }
}
impl WitnessIo for WitnessHandle {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.witness.lock().unwrap().len() as u64)
    }
    fn read_exact_at(&self, bytes: &mut [u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_read");
        if self.0.fault == Fault::WitnessRead {
            return Err(io::Error::other("witness read failure"));
        }
        bytes.copy_from_slice(&self.0.witness.lock().unwrap());
        Ok(())
    }
    fn write_all_at(&self, bytes: &[u8], _offset: u64) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_write");
        if self.0.fault == Fault::WitnessWriteBefore {
            return Err(io::Error::other("witness write failure"));
        }
        self.0.witness.lock().unwrap().copy_from_slice(bytes);
        if self.0.fault == Fault::WitnessWriteAfter {
            return Err(io::Error::other("witness write outcome unknown"));
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.events.lock().unwrap().push("witness_sync");
        if self.0.fault == Fault::WitnessSync {
            return Err(io::Error::other("witness sync failure"));
        }
        Ok(())
    }
}

#[test]
fn log_sync_precedes_witness_and_all_boundary_failures_propagate() {
    for (fault, expected) in [
        (Fault::LogSync, &["log_sync"][..]),
        (Fault::WitnessRead, &["log_sync", "witness_read"][..]),
        (
            Fault::WitnessWriteBefore,
            &["log_sync", "witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessWriteAfter,
            &["log_sync", "witness_read", "witness_write"][..],
        ),
        (
            Fault::WitnessSync,
            &["log_sync", "witness_read", "witness_write", "witness_sync"][..],
        ),
        (
            Fault::None,
            &["log_sync", "witness_read", "witness_write", "witness_sync"][..],
        ),
    ] {
        let id = [3; 16];
        let state = Arc::new(Shared {
            events: Mutex::new(Vec::new()),
            witness: Mutex::new(witness::encode_for_test(&id, 28)),
            fault,
        });
        let log = Log {
            file: Box::new(LogHandle(Arc::clone(&state))),
            log_id: Some(id),
            witness: Some(Witness::for_test(
                Box::new(WitnessHandle(Arc::clone(&state))),
                id,
                28,
            )),
        };
        let outcome = log.sync_data();
        assert_eq!(outcome.is_ok(), fault == Fault::None);
        assert_eq!(&*state.events.lock().unwrap(), expected);
    }
}

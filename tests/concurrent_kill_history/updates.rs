use super::*;

fn update_key(worker: usize) -> Vec<u8> {
    format!("repeated-key-{worker}").into_bytes()
}

fn update_value(worker: usize, sequence: usize) -> Option<Vec<u8>> {
    (sequence % 3 != 2).then(|| value(worker, sequence))
}

#[derive(Clone, Copy, Debug)]
struct UpdateEvent {
    worker: usize,
    sequence: usize,
    acknowledged: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct UpdateHistory {
    last_acknowledged: Option<usize>,
    pending: Option<usize>,
    next: usize,
}

impl UpdateHistory {
    fn observe(&mut self, event: UpdateEvent) -> Result<(), &'static str> {
        if event.sequence != self.next {
            return Err("unexpected update sequence");
        }
        if event.acknowledged {
            if self.pending != Some(event.sequence) {
                return Err("acknowledgement without matching start");
            }
            self.last_acknowledged = Some(event.sequence);
            self.pending = None;
            self.next += 1;
        } else if self.pending.replace(event.sequence).is_some() {
            return Err("another update is already in flight");
        }
        Ok(())
    }

    fn allows(self, worker: usize, actual: &Option<Vec<u8>>) -> bool {
        let acknowledged = self
            .last_acknowledged
            .and_then(|seq| update_value(worker, seq));
        actual == &acknowledged
            || self
                .pending
                .is_some_and(|seq| actual == &update_value(worker, seq))
    }
}

fn update_event(event: UpdateEvent) {
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "EMDA_UPDATE {} {} {} END",
        event.worker,
        event.sequence,
        if event.acknowledged { "ack" } else { "start" }
    )
    .unwrap();
    stdout.flush().unwrap();
}

fn update_until_killed(store: &impl TestStore) -> ! {
    let barrier = Barrier::new(5);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for sequence in 0..1000 {
                    update_event(UpdateEvent {
                        worker,
                        sequence,
                        acknowledged: false,
                    });
                    let key = update_key(worker);
                    if let Some(next) = update_value(worker, sequence) {
                        store.write(&key, &next).unwrap();
                    } else {
                        store.remove(&key).unwrap();
                    }
                    update_event(UpdateEvent {
                        worker,
                        sequence,
                        acknowledged: true,
                    });
                }
            });
        }
        barrier.wait();
        loop {
            std::thread::park();
        }
    });
    unreachable!()
}

#[test]
fn concurrent_update_kill_child() {
    let Some(parent) = std::env::var_os("EMDA_UPDATE_KILL_PARENT") else {
        return;
    };
    let parent = Path::new(&parent);
    let path = parent.join("store");
    if std::env::var("EMDA_UPDATE_KILL_BACKEND").unwrap() == "checkpoint" {
        let digest = fs::read(parent.join("digest")).unwrap().try_into().unwrap();
        let store = retry_lock(|| CheckpointStore::open(&path, parent.join("checkpoint"), &digest))
            .unwrap();
        update_until_killed(&store);
    } else {
        let store = retry_lock(|| Store::open(&path)).unwrap();
        update_until_killed(&store);
    }
}

fn parse_update(line: &str) -> Option<UpdateEvent> {
    let (_, message) = line.split_once("EMDA_UPDATE ")?;
    let mut parts = message.split_whitespace();
    let worker = parts.next()?.parse::<usize>().ok()?;
    let sequence = parts.next()?.parse::<usize>().ok()?;
    let acknowledged = match parts.next()? {
        "start" => false,
        "ack" => true,
        _ => return None,
    };
    if parts.next()? != "END" || parts.next().is_some() || worker >= 4 {
        return None;
    }
    Some(UpdateEvent {
        worker,
        sequence,
        acknowledged,
    })
}

fn check_updates<S: TestStore>(
    opened: io::Result<S>,
    histories: &[UpdateHistory; 4],
) -> Option<Vec<Option<Vec<u8>>>> {
    match opened {
        Ok(store) => {
            assert_eq!(store.read(b"safe").unwrap().as_deref(), Some(&b"guard"[..]));
            let mut values = Vec::new();
            for (worker, history) in histories.iter().enumerate() {
                let actual = store.read(&update_key(worker)).unwrap();
                assert!(
                    history.allows(worker, &actual),
                    "worker {worker}: {history:?} recovered {actual:?}"
                );
                values.push(actual);
            }
            Some(values)
        }
        Err(error) => {
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ),
                "unexpected recovery error: {error}"
            );
            None
        }
    }
}

fn run_update_case(format: Format, checkpoint_writer: bool, target: usize) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    fs::create_dir(&path).unwrap();
    if matches!(format, Format::V2) {
        fs::write(
            path.join("records.log"),
            include_bytes!("../fixtures/v2-empty-records.log"),
        )
        .unwrap();
        fs::write(
            path.join("commit.state"),
            include_bytes!("../fixtures/v2-empty-commit.state"),
        )
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    store.put(b"safe", b"guard").unwrap();
    let checkpoint = root.path().join("checkpoint");
    let digest = store.checkpoint_to(&checkpoint).unwrap();
    fs::write(root.path().join("digest"), digest).unwrap();
    let initial_log = fs::read(path.join("records.log")).unwrap();
    assert_eq!(
        &initial_log[..8],
        if matches!(format, Format::V2) {
            b"EMDALOG2"
        } else {
            b"EMDALOG3"
        }
    );
    let anchor = fs::read(path.join("commit.state")).unwrap();
    drop(store);

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "updates::concurrent_update_kill_child",
            "--nocapture",
        ])
        .env("EMDA_UPDATE_KILL_PARENT", root.path())
        .env(
            "EMDA_UPDATE_KILL_BACKEND",
            if checkpoint_writer {
                "checkpoint"
            } else {
                "ordinary"
            },
        )
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in io::BufReader::new(output).lines().map_while(Result::ok) {
            let event = parse_update(&line);
            assert!(
                !(line.contains("EMDA_UPDATE ")
                    && line.trim_end().ends_with(" END")
                    && event.is_none()),
                "malformed complete event: {line}"
            );
            if let Some(event) = event {
                if sender.send(event).is_err() {
                    break;
                }
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut events = Vec::new();
    let mut acknowledgements = 0;
    while acknowledgements < target {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match receiver.recv_timeout(remaining) {
            Ok(event) => {
                acknowledgements += usize::from(event.acknowledged);
                events.push(event);
            }
            Err(_) => break,
        }
    }
    let busy = acknowledgements >= target
        && matches!(Store::open(&path), Err(ref error) if error.kind() == io::ErrorKind::WouldBlock);
    let observed_prefix = fs::read(path.join("records.log")).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    reader.join().unwrap();
    events.extend(receiver.try_iter());
    assert!(
        busy,
        "active child must hold the store lock and reach the target"
    );
    let mut histories = [UpdateHistory::default(); 4];
    for event in events {
        histories[event.worker].observe(event).unwrap();
    }
    let log = fs::read(path.join("records.log")).unwrap();
    let witness = marker(&path, format);
    assert!(log.starts_with(&initial_log));
    assert!(log.starts_with(&observed_prefix));

    let first = if checkpoint_writer {
        check_updates(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &histories,
        )
    } else {
        check_updates(retry_lock(|| Store::open(&path)), &histories)
    };
    if first.is_none() {
        assert_eq!(marker(&path, format), witness);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    let second = if checkpoint_writer {
        check_updates(retry_lock(|| Store::open(&path)), &histories)
    } else {
        check_updates(
            retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)),
            &histories,
        )
    };
    assert_eq!(first, second, "recovery paths disagreed");
    if second.is_none() {
        assert_eq!(marker(&path, format), witness);
    }
    assert_eq!(fs::read(path.join("records.log")).unwrap(), log);
    if matches!(format, Format::V3) {
        assert_eq!(fs::read(path.join("commit.state")).unwrap(), anchor);
    }
    println!(
        "format={format:?} checkpoint_writer={checkpoint_writer} acknowledgements={} deletes={} pending={} recovered={}",
        histories.iter().map(|history| history.next).sum::<usize>(),
        histories
            .iter()
            .map(|history| history.next / 3)
            .sum::<usize>(),
        histories
            .iter()
            .filter(|history| history.pending.is_some())
            .count(),
        first.is_some()
    );
}

#[test]
fn acknowledged_updates_and_deletes_survive_process_kill_or_fail_closed() {
    for format in [Format::V2, Format::V3] {
        for checkpoint_writer in [false, true] {
            for target in [1, 8, 32, 128] {
                run_update_case(format, checkpoint_writer, target);
            }
        }
    }
}

#[test]
fn update_history_rejects_lost_acknowledgements_and_impossible_values() {
    let mut history = UpdateHistory::default();
    let event = UpdateEvent {
        worker: 0,
        sequence: 0,
        acknowledged: false,
    };
    assert!(
        history
            .observe(UpdateEvent {
                acknowledged: true,
                ..event
            })
            .is_err()
    );
    history.observe(event).unwrap();
    history
        .observe(UpdateEvent {
            acknowledged: true,
            ..event
        })
        .unwrap();
    assert!(history.allows(0, &update_value(0, 0)));
    assert!(!history.allows(0, &None));
    history
        .observe(UpdateEvent {
            sequence: 1,
            ..event
        })
        .unwrap();
    assert!(history.allows(0, &update_value(0, 0)));
    assert!(history.allows(0, &update_value(0, 1)));
    assert!(!history.allows(0, &update_value(0, 3)));
    history
        .observe(UpdateEvent {
            sequence: 1,
            acknowledged: true,
            ..event
        })
        .unwrap();
    history
        .observe(UpdateEvent {
            sequence: 2,
            ..event
        })
        .unwrap();
    history
        .observe(UpdateEvent {
            sequence: 2,
            acknowledged: true,
            ..event
        })
        .unwrap();
    assert!(history.allows(0, &None));
    assert!(!history.allows(0, &update_value(0, 1)));
}

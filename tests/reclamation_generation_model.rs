//! Identity rules for a future physical generation, checked without the engine.
//!
//! Today's read-set stores an offset, length, and checksum, and it skips that
//! comparison when its cached generation still equals the published one. That
//! is sound only while a published location is immutable and is not reused.
//! This file does not change that code, call it, or install reclamation. It
//! runs a private model of candidate rules and records when a rule would
//! accept a newer publication or return a newer retained payload.
//!
//! A rule stays sound through these scripts only when all of the following
//! hold together. The compared identity is the physical generation plus the
//! offset, length, and CRC. The skip generation and those addresses change in
//! one published view. An open capture pins every generation it names, and the
//! pin is released only when the capture closes. The pin counter is a checked
//! `u8`: the count that would exceed 255 is refused and does not wrap. A
//! retained generation is dropped only when nothing published still names it
//! and its counter is zero. A dropped id may be allocated again; a live pin
//! makes that drop impossible, so the open identity cannot reappear.
//!
//! The scripts are the whole state space under test. They are sequential:
//! each publication or drop is one view a reader can sample. There is no
//! thread schedule, memory order, crash cut, checksum calculation, range,
//! absence version, or grouped overflow record. Payload, offset, and CRC are
//! script inputs. Length is always 4. Two keys are enough for a torn pair.
//! Generation ids start at 1 and do not approach `u64::MAX`. A match is
//! unsound when an observed key's publication count changed, including a
//! publication that restores the old payload. A conflict while that count is
//! unchanged does not occur in these scripts. A snapshot read is unsound when
//! an open snapshot gets a different payload from the generation it captured.

const LEN: u16 = 4;
const KEY_A: u8 = 0;
const KEY_B: u8 = 1;
const PAYLOAD_A: u64 = 0xA7A7_0001;
const PAYLOAD_B: u64 = 0xB7B7_0002;
const PAYLOAD_C: u64 = 0xC7C7_0003;
const OFFSET_A: u32 = 8;
const OFFSET_B: u32 = 64;
const OFFSET_C: u32 = 96;
const CRC_A: u32 = 0xC1C1_0001;
const CRC_B: u32 = 0xC2C2_0002;
const CRC_C: u32 = 0xC3C3_0003;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityMode {
    /// Offset, length, and CRC, with no physical generation.
    Coordinates,
    /// Physical generation together with offset, length, and CRC.
    Generation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PinMode {
    /// `u8` saturates by refusing the increment that would wrap.
    Checked,
    /// `u8` wraps, so 256 live pins look like none.
    Wrapping,
}

#[derive(Clone, Copy)]
struct Rule {
    identity: IdentityMode,
    /// A dropped generation id may be allocated to a later publication.
    recycle_ids: bool,
    /// An open read-set increments the pin counter for generations it names.
    pin_reads: bool,
    pins: PinMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Address {
    generation: u64,
    offset: u32,
    len: u16,
    crc: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Observed {
    Coordinates {
        offset: u32,
        len: u16,
        crc: u32,
    },
    Generation {
        generation: u64,
        offset: u32,
        len: u16,
        crc: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Placement {
    key: u8,
    payload: u64,
    offset: u32,
    crc: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KeyState {
    present: bool,
    /// Counts publications of this key, including a restored payload.
    version: u64,
    payload: u64,
    address: Address,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Point {
    key: u8,
    version: u64,
    identity: Observed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadSet {
    open: bool,
    cached_epoch: u64,
    points: Vec<Point>,
    /// Generations this read-set named. Counted while it remains open, including
    /// when the rule did not increment the pin counter.
    captured: Vec<u64>,
    conflicted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Snapshot {
    open: bool,
    /// The counter was decremented while `open` remained true.
    released: bool,
    generation: u64,
    key: u8,
    payload: u64,
}

#[derive(Default)]
struct Segment {
    pins: u8,
    payloads: [Option<u64>; 2],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Match,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fault {
    step: &'static str,
    kind: FaultKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultKind {
    FalseMatch {
        key: u8,
        observed_version: u64,
        current_version: u64,
    },
    DroppedWhileCaptured {
        generation: u64,
        captures: usize,
        counter: u8,
    },
    ReusedBytes {
        generation: u64,
        expected: u64,
        observed: u64,
    },
    MissingBytes {
        generation: u64,
    },
}

struct Model {
    rule: Rule,
    step: &'static str,
    epoch: u64,
    next_generation: u64,
    free_ids: Vec<u64>,
    keys: [KeyState; 2],
    /// Index is the generation id. Slot 0 is unused.
    segments: Vec<Option<Segment>>,
    reads: Vec<ReadSet>,
    snapshots: Vec<Snapshot>,
    faults: Vec<Fault>,
}

fn place(key: u8, payload: u64, offset: u32, crc: u32) -> Placement {
    Placement {
        key,
        payload,
        offset,
        crc,
    }
}

fn observe(mode: IdentityMode, key: &KeyState) -> Observed {
    match mode {
        IdentityMode::Coordinates => Observed::Coordinates {
            offset: key.address.offset,
            len: key.address.len,
            crc: key.address.crc,
        },
        IdentityMode::Generation => Observed::Generation {
            generation: key.address.generation,
            offset: key.address.offset,
            len: key.address.len,
            crc: key.address.crc,
        },
    }
}

fn pinned(identity: IdentityMode, recycle_ids: bool) -> Rule {
    Rule {
        identity,
        recycle_ids,
        pin_reads: true,
        pins: PinMode::Checked,
    }
}

impl Model {
    fn new(rule: Rule) -> Self {
        Self {
            rule,
            step: "init",
            epoch: 0,
            next_generation: 1,
            free_ids: Vec::new(),
            keys: [KeyState {
                present: false,
                version: 0,
                payload: 0,
                address: Address {
                    generation: 0,
                    offset: 0,
                    len: 0,
                    crc: 0,
                },
            }; 2],
            segments: vec![None],
            reads: Vec::new(),
            snapshots: Vec::new(),
            faults: Vec::new(),
        }
    }

    fn at(&mut self, step: &'static str) {
        self.step = step;
    }

    fn key(&self, key: u8) -> &KeyState {
        &self.keys[usize::from(key)]
    }

    fn publish(&mut self, epoch: Option<u64>, placements: &[Placement]) {
        if let Some(epoch) = epoch {
            self.epoch = epoch;
        }
        if placements.is_empty() {
            return;
        }
        let generation = self.allocate();
        for placement in placements {
            let index = usize::from(placement.key);
            let key = &mut self.keys[index];
            key.present = true;
            key.version = key.version.checked_add(1).expect("publication count fits");
            key.payload = placement.payload;
            key.address = Address {
                generation,
                offset: placement.offset,
                len: LEN,
                crc: placement.crc,
            };
            self.ensure_segment(generation).payloads[index] = Some(placement.payload);
        }
    }

    fn allocate(&mut self) -> u64 {
        if self.rule.recycle_ids {
            if let Some(id) = self.free_ids.pop() {
                let index = usize::try_from(id).expect("generation fits");
                assert!(
                    self.segments[index].is_none(),
                    "recycled id is still retained"
                );
                return id;
            }
        }
        let id = self.next_generation;
        self.next_generation = id.checked_add(1).expect("generation id fits");
        id
    }

    fn ensure_segment(&mut self, generation: u64) -> &mut Segment {
        let index = usize::try_from(generation).expect("generation fits");
        if self.segments.len() <= index {
            self.segments.resize_with(index + 1, || None);
        }
        self.segments[index].get_or_insert_with(Segment::default)
    }

    fn pin(&mut self, generation: u64) -> bool {
        let mode = self.rule.pins;
        let pins = &mut self.ensure_segment(generation).pins;
        match mode {
            PinMode::Checked => match pins.checked_add(1) {
                Some(next) => {
                    *pins = next;
                    true
                }
                None => false,
            },
            PinMode::Wrapping => {
                *pins = pins.wrapping_add(1);
                true
            }
        }
    }

    fn unpin(&mut self, generation: u64) {
        let mode = self.rule.pins;
        let pins = &mut self.segments[usize::try_from(generation).expect("generation fits")]
            .as_mut()
            .expect("pin released on a retained generation")
            .pins;
        match mode {
            PinMode::Checked => *pins = pins.checked_sub(1).expect("pin count underflow"),
            PinMode::Wrapping => *pins = pins.wrapping_sub(1),
        }
    }

    fn counter(&self, generation: u64) -> u8 {
        let index = usize::try_from(generation).expect("generation fits");
        self.segments
            .get(index)
            .and_then(Option::as_ref)
            .map(|segment| segment.pins)
            .unwrap_or(0)
    }

    /// Open captures that still name `generation`, whether or not their
    /// counter slot is currently held.
    fn captures(&self, generation: u64) -> usize {
        let reads = self
            .reads
            .iter()
            .filter(|read| read.open && read.captured.contains(&generation))
            .count();
        let snapshots = self
            .snapshots
            .iter()
            .filter(|snapshot| snapshot.open && snapshot.generation == generation)
            .count();
        reads + snapshots
    }

    fn open_read_set(&mut self, keys: &[u8]) -> Option<usize> {
        let mut points = Vec::with_capacity(keys.len());
        let mut captured = Vec::new();
        for &key in keys {
            let state = self.key(key);
            assert!(state.present, "read-set observes a published key");
            let generation = state.address.generation;
            if !captured.contains(&generation) {
                captured.push(generation);
            }
            points.push(Point {
                key,
                version: state.version,
                identity: observe(self.rule.identity, state),
            });
        }
        if self.rule.pin_reads {
            let mut held = Vec::new();
            for &generation in &captured {
                if self.pin(generation) {
                    held.push(generation);
                } else {
                    for generation in held {
                        self.unpin(generation);
                    }
                    return None;
                }
            }
        }
        self.reads.push(ReadSet {
            open: true,
            cached_epoch: self.epoch,
            points,
            captured,
            conflicted: false,
        });
        Some(self.reads.len() - 1)
    }

    fn close_read_set(&mut self, index: usize) {
        assert!(self.reads[index].open, "read-set closed once");
        let captured = std::mem::take(&mut self.reads[index].captured);
        self.reads[index].open = false;
        if self.rule.pin_reads {
            for generation in captured {
                self.unpin(generation);
            }
        }
    }

    /// Equal epoch skips the location comparison. A match is then checked
    /// against the publication counts captured with those locations.
    fn validate(&mut self, index: usize) -> Verdict {
        assert!(self.reads[index].open, "closed read-set is not validated");
        if self.reads[index].conflicted {
            return Verdict::Conflict;
        }
        if self.reads[index].cached_epoch != self.epoch {
            let mode = self.rule.identity;
            let mut matched = true;
            for point in &self.reads[index].points {
                let key = self.key(point.key);
                if !key.present || observe(mode, key) != point.identity {
                    matched = false;
                    break;
                }
            }
            if !matched {
                self.reads[index].conflicted = true;
                return Verdict::Conflict;
            }
            self.reads[index].cached_epoch = self.epoch;
        }
        self.note_versions(index);
        Verdict::Match
    }

    fn note_versions(&mut self, index: usize) {
        let observed: Vec<(u8, u64)> = self.reads[index]
            .points
            .iter()
            .map(|point| (point.key, point.version))
            .collect();
        let step = self.step;
        for (key, observed_version) in observed {
            let current_version = self.key(key).version;
            if current_version != observed_version {
                self.faults.push(Fault {
                    step,
                    kind: FaultKind::FalseMatch {
                        key,
                        observed_version,
                        current_version,
                    },
                });
            }
        }
    }

    fn open_snapshot(&mut self, key: u8) -> Option<usize> {
        let state = self.key(key);
        assert!(state.present, "snapshot observes a published key");
        let generation = state.address.generation;
        let payload = self
            .payload(generation, key)
            .expect("published payload is retained");
        if !self.pin(generation) {
            return None;
        }
        self.snapshots.push(Snapshot {
            open: true,
            released: false,
            generation,
            key,
            payload,
        });
        Some(self.snapshots.len() - 1)
    }

    fn release_early(&mut self, index: usize) {
        let snapshot = &mut self.snapshots[index];
        assert!(snapshot.open, "early release applies to an open snapshot");
        assert!(!snapshot.released, "pin released once");
        snapshot.released = true;
        let generation = snapshot.generation;
        self.unpin(generation);
    }

    fn close_snapshot(&mut self, index: usize) {
        let snapshot = &mut self.snapshots[index];
        assert!(snapshot.open, "snapshot closed once");
        snapshot.open = false;
        if !snapshot.released {
            snapshot.released = true;
            let generation = snapshot.generation;
            self.unpin(generation);
        }
    }

    fn read_snapshot(&mut self, index: usize) -> Option<u64> {
        let snapshot = self.snapshots[index];
        assert!(snapshot.open, "closed snapshot is not read");
        match self.payload(snapshot.generation, snapshot.key) {
            Some(observed) if observed == snapshot.payload => Some(observed),
            Some(observed) => {
                self.faults.push(Fault {
                    step: self.step,
                    kind: FaultKind::ReusedBytes {
                        generation: snapshot.generation,
                        expected: snapshot.payload,
                        observed,
                    },
                });
                Some(observed)
            }
            None => {
                self.faults.push(Fault {
                    step: self.step,
                    kind: FaultKind::MissingBytes {
                        generation: snapshot.generation,
                    },
                });
                None
            }
        }
    }

    fn payload(&self, generation: u64, key: u8) -> Option<u64> {
        let index = usize::try_from(generation).expect("generation fits");
        self.segments.get(index).and_then(Option::as_ref)?.payloads[usize::from(key)]
    }

    fn referenced(&self, generation: u64) -> bool {
        self.keys
            .iter()
            .any(|key| key.present && key.address.generation == generation)
    }

    /// Retire one retained generation. A published address blocks the drop
    /// without a fault. A zero counter does not: open captures still name it.
    fn drop_generation(&mut self, generation: u64) -> bool {
        if self.referenced(generation) {
            return false;
        }
        let index = usize::try_from(generation).expect("generation fits");
        if self.segments.get(index).and_then(Option::as_ref).is_none() {
            return false;
        }
        let counter = self.counter(generation);
        if counter != 0 {
            return false;
        }
        let captures = self.captures(generation);
        if captures > 0 {
            self.faults.push(Fault {
                step: self.step,
                kind: FaultKind::DroppedWhileCaptured {
                    generation,
                    captures,
                    counter,
                },
            });
        }
        self.segments[index] = None;
        if self.rule.recycle_ids {
            self.free_ids.push(generation);
        }
        true
    }
}

struct Cycle {
    model: Model,
    reader: usize,
    dropped_first: bool,
    at_v2: Address,
    at_v2_payload: u64,
    verdict: Verdict,
}

/// Publish v1, move to v2, then publish v1's offset, length, CRC, and payload
/// again. Validation happens only at the end, which is the commit-time sample.
fn cycle(rule: Rule) -> Cycle {
    let mut model = Model::new(rule);
    model.at("v1");
    model.publish(Some(1), &[place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A)]);
    let reader = model.open_read_set(&[KEY_A]).expect("the first pin fits");
    model.at("v2");
    model.publish(Some(2), &[place(KEY_A, PAYLOAD_B, OFFSET_B, CRC_B)]);
    let at_v2 = model.key(KEY_A).address;
    let at_v2_payload = model.key(KEY_A).payload;
    model.at("drop-v1");
    let dropped_first = model.drop_generation(1);
    model.at("v1-again");
    model.publish(Some(3), &[place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A)]);
    model.at("validate");
    let verdict = model.validate(reader);
    Cycle {
        model,
        reader,
        dropped_first,
        at_v2,
        at_v2_payload,
        verdict,
    }
}

fn assert_v1_coordinates(address: Address) {
    assert_eq!(address.offset, OFFSET_A);
    assert_eq!(address.len, LEN);
    assert_eq!(address.crc, CRC_A);
}

#[test]
fn same_offset_len_crc_after_v1_v2_v1_needs_a_live_generation() {
    let coordinates = cycle(pinned(IdentityMode::Coordinates, false));
    assert!(
        !coordinates.dropped_first,
        "its pin still holds generation 1"
    );
    assert_eq!(coordinates.verdict, Verdict::Match);
    assert_eq!(coordinates.at_v2.generation, 2);
    assert_eq!(coordinates.at_v2.offset, OFFSET_B);
    assert_eq!(coordinates.at_v2_payload, PAYLOAD_B);
    assert_eq!(coordinates.model.key(KEY_A).payload, PAYLOAD_A);
    assert_eq!(coordinates.model.key(KEY_A).version, 3);
    let restored = coordinates.model.key(KEY_A).address;
    assert_eq!(restored.generation, 3, "the id was not recycled");
    assert_v1_coordinates(restored);
    assert_eq!(
        coordinates.model.reads[coordinates.reader].points[0].identity,
        observe(IdentityMode::Coordinates, coordinates.model.key(KEY_A)),
        "coordinates-only identity treats generation 3 as v1"
    );
    assert_eq!(
        coordinates.model.faults,
        vec![Fault {
            step: "validate",
            kind: FaultKind::FalseMatch {
                key: KEY_A,
                observed_version: 1,
                current_version: 3,
            },
        }]
    );

    let recycled = cycle(Rule {
        identity: IdentityMode::Generation,
        recycle_ids: true,
        pin_reads: false,
        pins: PinMode::Checked,
    });
    assert_eq!(recycled.verdict, Verdict::Match);
    assert!(recycled.dropped_first, "nothing pinned generation 1");
    let restored = recycled.model.key(KEY_A).address;
    assert_eq!(restored.generation, 1);
    assert_v1_coordinates(restored);
    assert_eq!(recycled.model.key(KEY_A).payload, PAYLOAD_A);
    assert_eq!(
        recycled.model.reads[recycled.reader].points[0].identity,
        Observed::Generation {
            generation: 1,
            offset: OFFSET_A,
            len: LEN,
            crc: CRC_A,
        }
    );
    assert_eq!(
        recycled.model.faults,
        vec![
            Fault {
                step: "drop-v1",
                kind: FaultKind::DroppedWhileCaptured {
                    generation: 1,
                    captures: 1,
                    counter: 0,
                },
            },
            Fault {
                step: "validate",
                kind: FaultKind::FalseMatch {
                    key: KEY_A,
                    observed_version: 1,
                    current_version: 3,
                },
            },
        ]
    );

    for recycle_ids in [false, true] {
        let mut held = cycle(pinned(IdentityMode::Generation, recycle_ids));
        assert_eq!(held.verdict, Verdict::Conflict);
        assert!(
            !held.dropped_first,
            "recycle_ids={recycle_ids}: a live pin refuses the drop"
        );
        let restored = held.model.key(KEY_A).address;
        assert_eq!(restored.generation, 3, "recycle_ids={recycle_ids}");
        assert_v1_coordinates(restored);
        assert_eq!(held.model.key(KEY_A).payload, PAYLOAD_A);
        assert_eq!(
            held.model.reads[held.reader].points[0].identity,
            Observed::Generation {
                generation: 1,
                offset: OFFSET_A,
                len: LEN,
                crc: CRC_A,
            }
        );
        assert_ne!(
            held.model.reads[held.reader].points[0].identity,
            observe(IdentityMode::Generation, held.model.key(KEY_A))
        );
        assert!(
            held.model.faults.is_empty(),
            "recycle_ids={recycle_ids}: {:?}",
            held.model.faults
        );
        held.model.at("validate-again");
        assert_eq!(held.model.validate(held.reader), Verdict::Conflict);
        held.model.at("close");
        held.model.close_read_set(held.reader);
        held.model.at("drop-after-close");
        assert!(
            held.model.drop_generation(1),
            "recycle_ids={recycle_ids}: close ends the pin"
        );
        assert!(held.model.faults.is_empty());
    }
}

#[test]
fn epoch_and_locations_are_one_published_view() {
    fn opened() -> (Model, usize) {
        let mut model = Model::new(pinned(IdentityMode::Generation, false));
        model.at("v1");
        model.publish(
            Some(1),
            &[
                place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A),
                place(KEY_B, PAYLOAD_B, OFFSET_C, CRC_C),
            ],
        );
        let reader = model.open_read_set(&[KEY_B]).expect("pin");
        (model, reader)
    }

    let (mut split_epoch, reader) = opened();
    let before = *split_epoch.key(KEY_B);
    split_epoch.at("epoch-only");
    split_epoch.publish(Some(2), &[]);
    assert_eq!(split_epoch.key(KEY_B), &before);
    assert_eq!(split_epoch.validate(reader), Verdict::Match);
    assert!(split_epoch.faults.is_empty(), "key B has not moved yet");
    assert_eq!(split_epoch.reads[reader].cached_epoch, 2);
    split_epoch.at("location-later");
    split_epoch.publish(None, &[place(KEY_B, PAYLOAD_C, OFFSET_B, CRC_B)]);
    assert_eq!(
        split_epoch.epoch, 2,
        "the second step does not bump the epoch"
    );
    assert_ne!(
        observe(IdentityMode::Generation, split_epoch.key(KEY_B)),
        split_epoch.reads[reader].points[0].identity,
        "generation and location changed after the epoch was cached"
    );
    assert_eq!(split_epoch.validate(reader), Verdict::Match);
    assert_eq!(
        split_epoch.faults,
        vec![Fault {
            step: "location-later",
            kind: FaultKind::FalseMatch {
                key: KEY_B,
                observed_version: 1,
                current_version: 2,
            },
        }]
    );

    let (mut split_location, reader) = opened();
    split_location.at("location-first");
    split_location.publish(None, &[place(KEY_B, PAYLOAD_C, OFFSET_B, CRC_B)]);
    assert_eq!(split_location.epoch, 1);
    assert_ne!(
        observe(IdentityMode::Generation, split_location.key(KEY_B)),
        split_location.reads[reader].points[0].identity,
        "the location changed while the skip epoch stayed put"
    );
    assert_eq!(split_location.validate(reader), Verdict::Match);
    assert_eq!(
        split_location.faults,
        vec![Fault {
            step: "location-first",
            kind: FaultKind::FalseMatch {
                key: KEY_B,
                observed_version: 1,
                current_version: 2,
            },
        }]
    );

    let (mut atomic, reader) = opened();
    atomic.at("move-other-key");
    atomic.publish(Some(2), &[place(KEY_A, PAYLOAD_C, OFFSET_B, CRC_B)]);
    assert_eq!(atomic.validate(reader), Verdict::Match);
    assert!(
        atomic.faults.is_empty(),
        "key B's generation and payload stay"
    );
    assert_eq!(atomic.key(KEY_B).version, 1);
    assert_eq!(atomic.key(KEY_B).address.generation, 1);
    atomic.at("move-observed-key");
    atomic.publish(Some(3), &[place(KEY_B, PAYLOAD_A, OFFSET_C, CRC_C)]);
    let moved = atomic.key(KEY_B).address;
    assert_eq!(moved.generation, 3);
    assert_eq!(moved.offset, OFFSET_C);
    assert_eq!(moved.len, LEN);
    assert_eq!(moved.crc, CRC_C);
    assert_eq!(atomic.key(KEY_B).payload, PAYLOAD_A);
    assert_eq!(atomic.validate(reader), Verdict::Conflict);
    assert_eq!(atomic.validate(reader), Verdict::Conflict);
    assert!(atomic.faults.is_empty());
}

#[test]
fn snapshot_pin_lasts_until_the_snapshot_closes() {
    let mut model = Model::new(Rule {
        identity: IdentityMode::Generation,
        recycle_ids: true,
        pin_reads: true,
        pins: PinMode::Checked,
    });
    model.at("v1");
    model.publish(Some(1), &[place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A)]);
    model.at("drop-published");
    assert!(
        !model.drop_generation(1),
        "the published address keeps generation 1 without a snapshot"
    );

    let snapshot = model.open_snapshot(KEY_A).expect("pin");
    model.at("v2");
    model.publish(Some(2), &[place(KEY_A, PAYLOAD_B, OFFSET_B, CRC_B)]);
    model.at("read-pinned");
    assert_eq!(model.read_snapshot(snapshot), Some(PAYLOAD_A));
    model.at("drop-pinned");
    assert!(!model.drop_generation(1));
    assert_eq!(model.counter(1), 1);
    assert!(model.faults.is_empty());

    let mut released = Model::new(model.rule);
    released.at("v1");
    released.publish(Some(1), &[place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A)]);
    let snapshot = released.open_snapshot(KEY_A).expect("pin");
    released.at("v2");
    released.publish(Some(2), &[place(KEY_A, PAYLOAD_B, OFFSET_B, CRC_B)]);
    released.at("release-early");
    released.release_early(snapshot);
    assert!(released.snapshots[snapshot].open);
    released.at("drop-early");
    assert!(released.drop_generation(1));
    released.at("read-missing");
    assert_eq!(released.read_snapshot(snapshot), None);
    released.at("reuse-id");
    released.publish(Some(3), &[place(KEY_A, PAYLOAD_C, OFFSET_A, CRC_A)]);
    assert_eq!(released.key(KEY_A).address.generation, 1);
    assert_v1_coordinates(released.key(KEY_A).address);
    released.at("read-reused");
    assert_eq!(released.read_snapshot(snapshot), Some(PAYLOAD_C));
    assert_eq!(
        released.faults,
        vec![
            Fault {
                step: "drop-early",
                kind: FaultKind::DroppedWhileCaptured {
                    generation: 1,
                    captures: 1,
                    counter: 0,
                },
            },
            Fault {
                step: "read-missing",
                kind: FaultKind::MissingBytes { generation: 1 },
            },
            Fault {
                step: "read-reused",
                kind: FaultKind::ReusedBytes {
                    generation: 1,
                    expected: PAYLOAD_A,
                    observed: PAYLOAD_C,
                },
            },
        ]
    );

    model.at("close");
    model.close_snapshot(snapshot);
    model.at("drop-after-close");
    assert!(model.drop_generation(1));
    model.at("reuse-after-close");
    model.publish(Some(3), &[place(KEY_A, PAYLOAD_C, OFFSET_A, CRC_A)]);
    assert_eq!(model.key(KEY_A).address.generation, 1);
    let replacement = model.open_snapshot(KEY_A).expect("new pin");
    model.at("read-replacement");
    assert_eq!(model.read_snapshot(replacement), Some(PAYLOAD_C));
    assert!(model.faults.is_empty());
    assert_eq!(model.counter(1), 1);
    assert_eq!(model.captures(1), 1);
}

#[test]
fn pin_count_overflow_refuses_instead_of_dropping_a_live_capture() {
    fn fill(pins: PinMode) -> Model {
        let mut model = Model::new(Rule {
            identity: IdentityMode::Generation,
            recycle_ids: true,
            pin_reads: true,
            pins,
        });
        model.at("v1");
        model.publish(Some(1), &[place(KEY_A, PAYLOAD_A, OFFSET_A, CRC_A)]);
        model.at("fill-pins");
        for _ in 0..usize::from(u8::MAX) {
            assert!(model.open_snapshot(KEY_A).is_some());
        }
        assert_eq!(model.counter(1), u8::MAX);
        assert_eq!(model.captures(1), usize::from(u8::MAX));
        model
    }

    let mut wrapping = fill(PinMode::Wrapping);
    wrapping.at("wrap-pin");
    let reader = wrapping.open_read_set(&[KEY_A]).expect("wrapping pin");
    assert_eq!(wrapping.counter(1), 0);
    assert_eq!(wrapping.captures(1), usize::from(u8::MAX) + 1);
    wrapping.at("v2");
    wrapping.publish(Some(2), &[place(KEY_A, PAYLOAD_B, OFFSET_B, CRC_B)]);
    wrapping.at("drop-wrapped");
    assert!(wrapping.drop_generation(1));
    wrapping.at("v1-again");
    wrapping.publish(Some(3), &[place(KEY_A, PAYLOAD_C, OFFSET_A, CRC_A)]);
    assert_eq!(wrapping.key(KEY_A).address.generation, 1);
    assert_v1_coordinates(wrapping.key(KEY_A).address);
    wrapping.at("validate-wrapped");
    assert_eq!(wrapping.validate(reader), Verdict::Match);
    wrapping.at("read-wrapped");
    assert_eq!(wrapping.read_snapshot(0), Some(PAYLOAD_C));
    assert_eq!(
        wrapping.faults,
        vec![
            Fault {
                step: "drop-wrapped",
                kind: FaultKind::DroppedWhileCaptured {
                    generation: 1,
                    captures: usize::from(u8::MAX) + 1,
                    counter: 0,
                },
            },
            Fault {
                step: "validate-wrapped",
                kind: FaultKind::FalseMatch {
                    key: KEY_A,
                    observed_version: 1,
                    current_version: 3,
                },
            },
            Fault {
                step: "read-wrapped",
                kind: FaultKind::ReusedBytes {
                    generation: 1,
                    expected: PAYLOAD_A,
                    observed: PAYLOAD_C,
                },
            },
        ]
    );

    let mut checked = fill(PinMode::Checked);
    checked.at("refuse-pin");
    assert!(checked.open_snapshot(KEY_A).is_none());
    assert_eq!(checked.snapshots.len(), usize::from(u8::MAX));
    assert_eq!(checked.counter(1), u8::MAX);
    assert_eq!(checked.captures(1), usize::from(u8::MAX));
    checked.at("close-one");
    checked.close_snapshot(0);
    assert_eq!(checked.counter(1), u8::MAX - 1);
    checked.at("pin-after-close");
    let reader = checked.open_read_set(&[KEY_A]).expect("a free slot");
    assert_eq!(checked.counter(1), u8::MAX);
    assert_eq!(checked.reads[reader].points[0].version, 1);
    checked.at("v2");
    checked.publish(Some(2), &[place(KEY_A, PAYLOAD_B, OFFSET_B, CRC_B)]);
    checked.at("drop-full");
    assert!(!checked.drop_generation(1));
    checked.at("restore-coordinates");
    checked.publish(Some(3), &[place(KEY_A, PAYLOAD_C, OFFSET_A, CRC_A)]);
    assert_eq!(checked.key(KEY_A).address.generation, 3);
    assert_v1_coordinates(checked.key(KEY_A).address);
    assert_eq!(checked.key(KEY_A).payload, PAYLOAD_C);
    checked.at("validate-held");
    assert_eq!(checked.validate(reader), Verdict::Conflict);
    checked.at("read-still-held");
    assert_eq!(checked.read_snapshot(1), Some(PAYLOAD_A));
    assert!(checked.faults.is_empty());
    assert_eq!(checked.counter(1), u8::MAX);
    assert_eq!(checked.captures(1), usize::from(u8::MAX));
}

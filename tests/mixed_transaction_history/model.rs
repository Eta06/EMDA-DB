//! Independent serial-history oracle and its negative controls.
use std::collections::HashSet;

pub(super) type State = [Option<u8>; 3];
pub(super) const INITIAL: State = [Some(0), None, Some(0)];

#[derive(Clone, Copy, Debug)]
pub(super) enum Operation {
    RangeTransaction,
    PointTransaction,
    Put,
    Delete,
    Compare,
    Batch,
    Scan,
    GetMany,
}

pub(super) const OPERATIONS: [Operation; 8] = [
    Operation::RangeTransaction,
    Operation::PointTransaction,
    Operation::Put,
    Operation::Delete,
    Operation::Compare,
    Operation::Batch,
    Operation::Scan,
    Operation::GetMany,
];

#[derive(Clone, Copy, Debug)]
pub(super) enum Outcome {
    Transaction(State),
    Conflict,
    Done,
    Compared(bool),
    Read(State),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Event {
    pub(super) operation: Operation,
    pub(super) outcome: Outcome,
    pub(super) start: usize,
    pub(super) finish: usize,
}

// A point transaction only observes b; the other positions are intentionally
// ignored by the model. It writes c based on whether that point exists.
fn apply_point(state: &mut State) {
    state[2] = if state[1].is_some() { None } else { Some(9) };
}

fn apply_range(state: &mut State) {
    state[0] = if state[0].is_some() { None } else { Some(1) };
    state[2] = state[1].map(|value| value + 10);
}

pub(super) fn serializes(events: &[Event], initial: State) -> bool {
    fn search(events: &[Event], used: u16, state: State, seen: &mut HashSet<(u16, State)>) -> bool {
        if used.count_ones() as usize == events.len() {
            return true;
        }
        if !seen.insert((used, state)) {
            return false;
        }
        for (index, event) in events.iter().enumerate() {
            let bit = 1_u16 << index;
            if used & bit != 0
                || events.iter().enumerate().any(|(other, predecessor)| {
                    used & (1_u16 << other) == 0 && predecessor.finish < event.start
                })
            {
                continue;
            }
            let mut next = state;
            match (event.operation, event.outcome) {
                (Operation::RangeTransaction | Operation::PointTransaction, Outcome::Conflict) => {}
                (Operation::RangeTransaction, Outcome::Transaction(observed))
                    if observed == state =>
                {
                    apply_range(&mut next);
                }
                (Operation::PointTransaction, Outcome::Transaction(observed))
                    if observed[1] == state[1] =>
                {
                    apply_point(&mut next);
                }
                (Operation::Put, Outcome::Done) => next[1] = Some(1),
                (Operation::Delete, Outcome::Done) => next[1] = None,
                (Operation::Compare, Outcome::Compared(success))
                    if success == state[1].is_none() =>
                {
                    if success {
                        next[1] = Some(2);
                    }
                }
                (Operation::Batch, Outcome::Done) => {
                    next[0] = Some(6);
                    next[2] = None;
                }
                (Operation::Scan | Operation::GetMany, Outcome::Read(observed))
                    if observed == state => {}
                _ => continue,
            }
            if search(events, used | bit, next, seen) {
                return true;
            }
        }
        false
    }
    assert!(events.len() < u16::BITS as usize);
    search(events, 0, initial, &mut HashSet::new())
}

#[test]
fn model_rejects_stale_range_and_absent_point_after_a_completed_insert() {
    for transaction in [Operation::RangeTransaction, Operation::PointTransaction] {
        let events = [
            Event {
                operation: Operation::Put,
                outcome: Outcome::Done,
                start: 0,
                finish: 1,
            },
            Event {
                operation: transaction,
                outcome: Outcome::Transaction(INITIAL),
                start: 2,
                finish: 3,
            },
        ];
        assert!(!serializes(&events, INITIAL));
    }
}

#[test]
fn model_checks_atomic_batches_compare_results_and_real_time_order() {
    let event = |operation, outcome, start, finish| Event {
        operation,
        outcome,
        start,
        finish,
    };
    let fractured = [
        event(Operation::Batch, Outcome::Done, 0, 1),
        event(
            Operation::Scan,
            Outcome::Read([Some(6), None, Some(0)]),
            2,
            3,
        ),
    ];
    assert!(!serializes(&fractured, INITIAL));
    let wrong_comparison = [event(Operation::Compare, Outcome::Compared(false), 0, 1)];
    assert!(!serializes(&wrong_comparison, INITIAL));
    let legal = [
        event(Operation::Put, Outcome::Done, 0, 1),
        event(
            Operation::GetMany,
            Outcome::Read([Some(0), Some(1), Some(0)]),
            2,
            3,
        ),
    ];
    assert!(serializes(&legal, INITIAL));
}

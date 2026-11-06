//! Real-time serial model for committed, expired, and uncertain transactions.
use super::{Event, Outcome, State};
use std::collections::HashSet;

fn committed(state: State, operation: usize, observed: [u8; 2]) -> Option<State> {
    (state.pair == observed).then(|| State {
        pair: [state.pair[0] + 1, state.pair[1] + 1],
        marks: state.marks | (1 << operation),
    })
}

pub(super) fn serializes(events: &[Event], initial: State, final_state: State) -> bool {
    fn search(
        events: &[Event],
        final_state: State,
        used: u16,
        state: State,
        visited: &mut HashSet<(u16, State)>,
    ) -> bool {
        if used.count_ones() as usize == events.len() {
            return state == final_state;
        }
        if !visited.insert((used, state)) {
            return false;
        }
        for (index, event) in events.iter().enumerate() {
            let bit = 1_u16 << index;
            if used & bit != 0
                || events.iter().enumerate().any(|(other, predecessor)| {
                    used & (1 << other) == 0 && predecessor.end < event.start
                })
            {
                continue;
            }
            let mut candidates = [None, None];
            match event.outcome {
                Outcome::Committed(observed) => {
                    candidates[0] = committed(state, event.operation, observed);
                }
                Outcome::Expired { .. } => candidates[0] = Some(state),
                Outcome::Unknown { staged, .. } => {
                    candidates[0] = Some(state);
                    candidates[1] =
                        staged.and_then(|observed| committed(state, event.operation, observed));
                }
                Outcome::Batch => {
                    candidates[0] = Some(State {
                        pair: [5, 5],
                        ..state
                    })
                }
                Outcome::Put => {
                    candidates[0] = Some(State {
                        pair: [2, state.pair[1]],
                        ..state
                    })
                }
                Outcome::Read(pair) if pair == state.pair => candidates[0] = Some(state),
                Outcome::Read(_) => {}
            }
            if candidates
                .into_iter()
                .flatten()
                .any(|next| search(events, final_state, used | bit, next, visited))
            {
                return true;
            }
        }
        false
    }
    assert!(events.len() < u16::BITS as usize);
    search(events, final_state, 0, initial, &mut HashSet::new())
}

#[test]
fn reference_model_checks_expired_and_uncertain_outcomes() {
    let initial = State {
        pair: [3, 2],
        marks: 0,
    };
    let final_state = State {
        pair: [4, 3],
        marks: 1,
    };
    let mut event = Event {
        operation: 0,
        start: 0,
        end: 1,
        outcome: Outcome::Expired { staged: true },
    };
    assert!(!serializes(&[event], initial, final_state));
    event.outcome = Outcome::Unknown {
        staged: Some(initial.pair),
        timed_out: true,
    };
    assert!(serializes(&[event], initial, final_state));
    assert!(serializes(&[event], initial, initial));
    assert!(!serializes(
        &[event],
        initial,
        State {
            pair: [4, 2],
            marks: 1
        }
    ));
}

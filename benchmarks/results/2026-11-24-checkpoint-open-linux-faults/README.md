# Linux checkpoint recovery witness faults

[Filesystem EIO CI](https://github.com/Eta06/EMDA-DB/actions/runs/45339422067)
passed at `308e8e7`. The [raw results](rebuild-witness-faults.jsonl)
include 38 returned syscall errors across ordinary and checkpoint opens
and three rebuild paths. Fourteen cases exercise the new explicit and
managed checkpoint open APIs.

Both checkpoint opens classified an error before the witness write as
`NotAdopted`, and witness write or sync errors as
`TailAdoptionOutcomeUnknown` on V2 and V3. Managed open also returned
`NotAdopted` on a failed selection file sync. The campaign checked the
actual witness bytes, unchanged log and selection, and the correct value
on a clean retry. These checks cover returned Linux syscall errors.

# Streaming retained-base proposal

Status: non-authoritative export implemented. This specifies an export format; it does
not change existing logs, checkpoints or the experimental `EMDARB01` codec.
Export is non-destructive and confers no authority to recover, switch, or
remove history. Authoritative recovery below is a proposed protocol, gated on
explicit decisions and implementation of all its prerequisites.

A streaming validator exercises the wire checks below. It returns
self-consistent metadata only and does not establish
source provenance, immutable input, trusted authority or safe publication.
Validation rejects a base ID equal to the source ID, matching the exporter;
previously the inspection codec accepted such files. Distinct IDs alone do not
prove freshness, uniqueness, provenance, or authority.
Single-key lookup captures the requested budgeted value from the same bytes it
hashes, then returns it only after checking the complete stream and the
caller-supplied digest, IDs and boundary. A budget failure can precede full
validation. This inspection API does not establish witness provenance or
authorize recovery or deletion.

## Wire format and streaming contract

Proposed `EMDARB02`, little endian, canonical fixed-width integers. Header:
8-byte magic, u32 version (=2), u32 header length (=112), 16-byte nonzero
source generation ID, 16-byte nonzero new base ID distinct from the source ID, u64 boundary, u64 entry
count, u64 live key bytes, u64 historical peak count, u64 historical peak key
bytes, u64 encoded entry bytes, u64 flags (=0), and 8 reserved zero bytes.
This totals 112 bytes. Boundary is an exact committed source-log end offset,
including all framing, qualified by source ID; it is not a sequence number or
witness on its own. Entries are `(u32 key_len, u32 value_len, key, value)`;
keys are 1..1024 bytes and strictly increasing in unsigned byte order, values
are 0..16 MiB inclusive. Empty value is present; absence means deleted. A
32-byte SHA-256 trailer hashes the domain string `EMDA retained base v2\0`
followed by the exact header and entry bytes; it does not include itself.
There is exactly one trailer and EOF. Unknown version, header size, flags or
reserved bits fail closed. No implicit conversion from v1 is permitted.
Encoded entry bytes means the sum of both four-byte length fields, key bytes
and value bytes for every entry. Checked file length is exactly 112 plus that
sum plus 32 trailer bytes. Retry ordinary partial reads; EOF before all
required bytes is an error.

The count, byte totals and peaks are u64 with checked arithmetic; header totals
must equal observed totals, peaks must be at least corresponding live totals,
and all lengths, file offsets, sums and host conversions must be checked.
There is no arbitrary 100,000-entry, 16 MiB-file or 1 MiB-value cap. Caller
provides finite maximum file bytes, count, key bytes and work budget before
reading; reject excess without allocating from untrusted lengths. Defaults and
compatibility policy require a decision. Existing stores exceeding a selected
budget must remain usable through the old path, not be silently truncated.

Writer knows header totals from a stable source traversal or writes a private
seekable staging file, patches the header and performs a complete sequential
hash pass after patching. Reader uses a fixed buffer (e.g. 64 KiB), at most two
1024-byte keys, and incremental hashing; value chunks are never accumulated.
The exact memory bound excludes the caller's pre-existing index and any
explicitly budgeted sink. A sink is transactional staging, not a live index:
no entry is visible as accepted until all counts, EOF, digest and external
binding validate. Abort discards staging on any error. A second pass or a
bounded on-disk staging index is acceptable; returning an unbounded Vec is
not. Detect short reads, I/O errors, extra bytes, changing files, duplicate or
out-of-order keys, impossible counts, and arithmetic overflow. For immutable
files use a pinned descriptor and prevent concurrent modification by protocol;
a descriptor alone does not guarantee immutability. Read and hash every byte
on each validation; a cached digest is not verification of current bytes.
Resource limits bound memory and admitted bytes, not wall-clock time or I/O
latency. Disk space for staging and an index must also be budgeted.
Header peaks being at least live totals is necessary but cannot prove their
provenance. A self-consistent digest cannot establish that an export faithfully
represents the source; that requires verified replay or an independently
trusted binding.
These codec bounds exclude source verification, which currently retains up to
1,024 batch child keys, and any source index, external sort, run cursors,
descriptors and staging files. A two-pass consumer must enforce immutability
between passes; an open descriptor alone is insufficient. Specify sink commit,
abort, partial-write and cleanup behavior before implementation.

Historical peaks describe maxima of logical live key count and summed key
length over the whole source lineage through the boundary, including every
ordered child of each validated atomic batch, as current replay does. Carry both
forward as the maximum of the previous peak and each intermediate child state;
replay later complete batches updates them similarly. The batch is accepted
atomically even though its children count separately for the high-water marks.
Do not infer either peak from current state.
Existing replay records the count peak but does not retain a historical byte
peak. Key-byte peak provenance must be reconstructed by verified ordered full
replay, or the export must
mark provenance unavailable and be ineligible for authoritative use. This
proposal chooses rejection for missing provenance, not a fabricated zero.
The internal full-log index verifier now reconstructs both peaks in ordered
replay and checks the resulting index and witness. It builds a full in-memory
index and is used by the bounded retained-base exporter; it is not bounded recovery.

## Producing and authenticating an export

For the first implementation hold the writer barrier through source verification
and stable view capture; an advancing V2 or V3 witness cannot otherwise be
assumed to authenticate a historical boundary. Under that barrier select a
fully committed boundary and capture a
consistent generation/index view with its file pins. Verify the source according
to current recovery's full corruption and witness rules, including bytes
superseded by later records, through that boundary. Stream sorted live state
from that same view; validate referenced values and preserve grouped records'
logical effects. A stable view must not rely on snapshot limits silently
excluding larger stores; use an explicitly budgeted external traversal or fail.
Do not admit concurrent appends until verification and stable capture finish.
A future concurrent export needs a protocol for capturing and validating the
boundary's witness and immutable source bytes. Later appends remain in the
source and are not included. The exported
state represents all complete acknowledged batches through the boundary and
no partial batch; it may include a complete commit whose acknowledgement was
lost. It makes no claim about later writes. A source read or sync error aborts.

The embedded SHA-256 detects corruption only. An independently trusted record
must bind format/version, store lineage, source ID and boundary, new base ID,
counts and peaks, exact byte length and computed digest, plus the committed
witness state at that boundary. Validate the actual file against this trusted
record and validate the witness chain/source binding; a self-signed trailer or
manifest is insufficient. For an offline export without such a record, label it
untrusted and never use it as authority. Exact witness representation and how
independent trust is provisioned are decisions, not supplied by this codec.

The exporter assumes the source bytes, witness and directories are trusted and
stable against uncoordinated mutation throughout verification and publication;
the advisory lock and writer barrier only exclude cooperating writers. The caller
must place the destination outside the source directory. Store does not retain
its source path, so the exporter cannot enforce that precondition, including
through directory aliases. Its output is non-authoritative and cannot be used
for recovery without the independent binding described above.

The exporter holds the writer barrier throughout verification and publication.
It requires caller-provided replay and output budgets and a nonzero base ID
distinct from the source ID. The caller is responsible for ID uniqueness; no
allocation registry or independent trust is provided. A publication failure
after the attempt begins is reported as outcome unknown.

## Read-only indexed inspection

`retained_candidate::IndexedCandidate` verifies the base and independently
retained candidate binding before returning a handle. The verification pass
also builds an in-memory directory containing one anchor key, file range and
SHA-256 digest per block. A block spans at most 128 KiB of whole entries or
one larger entry. Explicit block and anchor-byte limits bound the directory.
Opening scans all base bytes with a fixed value buffer.

The handle retains the validated base descriptor. A point read searches the
anchor keys and reads at most one block. An ordered page uses the store's entry
and response-byte limits, including exclusive key bounds. A read hashes each
touched block while capturing requested result bytes. A failure discards the
whole result. The base is expected to remain immutable. The header, trailer,
companion files and untouched blocks are checked at open only, and changing
the file length fails subsequent nonempty reads. A pathname replacement does
not redirect the captured descriptor. Directory allocations, read scratch
space, result buffers and allocator overhead have separate costs.

The directory is rebuilt at open. `retained_candidate::IndexedSuffix` adds
a read-only view of an independently bound detached suffix over that base.
The suffix is verified in a private byte buffer subject to the suffix-byte
limit. Ordered DB01 and DB02 operations update a sorted overlay of keys and
offsets into the verified buffer. Tombstones remain in the overlay and count
toward its explicit key and entry limits. Overlay work is charged separately
from the base verification. A merged page applies one result budget across
both sources, with suffix replacements and deletions taking precedence.
Overlay points read only the captured suffix. New-history restore still uses
its materialized replay path. Writable recovery still needs a committed
witness and durable generation selection.

## Future authority, recovery and publication (not implemented)

Authoritative cutover remains prohibited until the old-reader gate and backup
policy cover empty or legacy-looking suffixes and independently opened stale
copies. A sidecar alone cannot make those readers fail closed.

A fresh durable generation ID is allocated before staging, never reused for a
relocation even when bytes match. Require a durable globally unique allocation
registry for the store lineage, checked against all current and retired IDs;
collision retries allocate a new ID, and entropy/registry/counter exhaustion or
uncertain allocation fails closed. Restores and clones must preserve the
registry and lineage or obtain a new externally registered lineage before any
writes. An ID is not a path, offset, checksum, inode or process counter.

A future suffix begins at the base boundary and is explicitly bound to base ID,
source boundary and trusted digest. Recovery verifies base bytes and binding,
then validates and replays complete suffix batches in order with existing DB02,
grouped-record, delete and witness rules. Apply each batch atomically; a torn
or uncertain last append follows the existing witnessed commit protocol, not a
new rule that treats arbitrary truncation as harmless. A committed witness with
missing or corrupt data fails closed. No acknowledged batch may fall between
the base boundary and suffix; synchronize boundary selection, suffix capture
and final cutover with the writer barrier. Finalize and verify the suffix and
its witness through the cutover before admitting writes under the new view.

A candidate protocol requires: (1) create exclusive private base, suffix and
manifest files, write, sync each, re-read and verify all bytes and bindings;
(2) sync the containing directory after durable names are installed; (3) under
the writer barrier finish and sync the suffix and candidate manifest, verify
again, then commit a trusted independent authority record binding the exact
candidate and its witness; (4) atomically publish the matching in-memory
identity/index/handles and admit writers only after authority is confirmed.
The independent authority must support atomic durable compare-and-swap from
old to new, or an equivalent specified protocol with unambiguous recovery;
a local manifest epoch or rename alone is not rollback protection. The exact
ordering of its witness advancement with writable suffix appends must be
specified by that authority before implementation. No fallback to an older
record is allowed when trusted authority names a missing or corrupt candidate.

Before authority commit, a crash leaves the old authority selected and the
candidate is merely staging; after a confirmed commit, reopen selects and fully
verifies the candidate. A crash or error during any write, sync, rename,
directory sync, authority update or acknowledgement has an uncertain outcome:
stop writes, retain both copies, query the trusted authority on reopen and
verify its exact choice; if the authority cannot resolve the outcome, fail
closed. Never delete either candidate on uncertainty. Failure after authority
commit but before memory publication is resolved from the trusted record, not
by resuming the old writer. Crash-safe publication depends on documented
filesystem sync/rename guarantees and trustworthy authority semantics; neither
is proven here. Backup must include the matching base, suffix, manifest,
witness and trusted authority/identity lineage at one consistent boundary;
restore validates all bindings and anti-rollback policy before opening.

Old-reader fail-closed behavior is a hard gate: the *actual entry point old
binaries open*, not merely a sidecar, must contain a format/version they reject
before any suffix could be interpreted as a complete log. Verify this against
every supported old reader, including missing sidecar and stale backup cases.
If no safe marker and atomic installation can be demonstrated, authoritative
migration is prohibited. This design does not claim the current readers provide
that property. Legacy stores require explicit migration after full verification;
no in-place reinterpretation or downgrade to suffix-only recovery is allowed.

Capture the identity, index and all file handles under one state/admission lock;
pin before releasing it, and use those handles for every direct read, snapshot,
transaction and in-flight I/O. Publish the replacement under the same lock;
checked pin overflow rejects admission. Existing readers retain the old view,
including after snapshot expiry until active I/O completes. Qualify all physical
locations and transaction observations by generation; equality across different
IDs fails even for identical offsets/checksums. Old files can only be retired
after confirmed durable authority, successful verification, and all pins and
handles are gone; stalled readers may delay retirement indefinitely. Define
fork, duplicated descriptor and open-file-description lock ownership before
cleanup; do not unlock while another owner relies on the lock. This proposal
authorizes no deletion or reclamation.

## Decisions and focused implementation

Decide trusted authority and witness API, suffix commit ordering and uncertain
commit resolution; identity allocation/clone rules; supported old-reader marker
and migration mechanism; finite count, bytes, disk and I/O budgets and their
backpressure behavior; legacy key-byte-peak reconstruction; filesystem and
backup guarantees; and process/fork lock ownership. Until resolved, only the
non-destructive export experiment is eligible for implementation.

1. Implement isolated v2 streaming writer/validator with a transactional test
   sink and explicit budgets, no store wiring. Test empty state/value, a 16 MiB
   value, 1024-byte keys, over 100,000 entries, and files over 16 MiB; verify
   bounded buffers and no allocation from declared lengths.
2. Test every truncation on small fixtures, extra bytes, bit flips, validly
   rehashed malformed headers/entries, order/duplicate violations, overflow,
   wrong totals/peaks, injected short reads and sink failures. Assert no staged
   result becomes visible on failure and trusted digest/witness mismatch fails.
3. Add offline verified export from a pinned stable boundary, with full source
   verification and both historical peaks. Test deletes, repeated keys, grouped
   operations, atomic batches, concurrent appends and lost acknowledgements.
   Compare replayed logical state and peaks to source; exceed each budget.
4. Only after the decisions above, prototype authority independently: inject
   crash and sync failures at every boundary, witness rollback/mismatch,
   missing files, cross-generation ABA, concurrent pin/cutover, stale readers,
   old-reader downgrade and consistent backup/restore. Exercise actual engine
   paths and Linux lock lifetimes. Measure peak memory, disk and I/O; do not
   infer power-loss durability or off-host restore from unit tests.

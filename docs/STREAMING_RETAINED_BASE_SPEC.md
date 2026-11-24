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
SHA-256 digest per block. Anchor keys share one byte buffer and each block
stores a range into it. A block spans at most 128 KiB of whole entries or
one larger entry. Explicit block and anchor-byte limits bound the directory
capacities requested from the allocator.
Opening scans all base bytes through a fixed input buffer. The file and
block digests use the admitted value slices directly from that buffer.

The handle retains the validated base descriptor. A point read searches the
anchor keys and reads at most one block. An oversized block holds exactly one
entry. Its verified anchor can prove that a different key is absent without
reading the value. An ordered page uses the store's entry
and response-byte limits, including exclusive key bounds. A read hashes each
touched block while capturing requested result bytes. A failure discards the
whole result. The base is expected to remain immutable. The header, trailer,
companion files and untouched blocks are checked at open only, and changing
the file length fails subsequent nonempty reads. A pathname replacement does
not redirect the captured descriptor. Directory allocations, read scratch
space, result buffers and allocator overhead have separate costs.

The directory is rebuilt at open. Base-file verification admits the header
before buffering up to the declared body length and one EOF probe. Indexed
reads authenticate and visit value slices from the same block input buffer.
Rebasing buffers
writes to its private staged file and flushes before syncing and validating
that output. All three paths still validate the complete required bytes.

`retained_candidate::IndexedSuffix` adds
a read-only view of an independently bound detached suffix over that base.
The default open captures and verifies the suffix in a private byte buffer
subject to the suffix-byte limit. File-backed open instead retains the opened
suffix descriptor and streams the same bound suffix through the fixed-size
recovery buffer. Ordered DB01 and DB02 operations update a sorted overlay of
keys and verified value locations. Tombstones remain in the overlay and count
toward its explicit key and entry limits. Overlay work is charged separately
from the base verification. A merged page applies one result budget across
both sources, with suffix replacements and deletions taking precedence.
A merged scan can fill its page with suffix rows strictly before the next
verified base anchor. In that case it leaves the next block unread. Pages
that need a base block still authenticate all of its bytes.
The file-backed view checks the retained descriptor's length and authenticates
returned suffix values against their captured digests. The base and suffix
files must stay immutable for that view's lifetime. These checks cover the
values returned by each read. They do not revalidate unrelated suffix history
after open. Writable recovery still needs a committed witness and durable
generation selection.

New-history restore verifies the full suffix digest before base admission
and checks it again during ordered replay. Before scanning base values,
it uses the declared base entry count and ordered suffix operations to
reject an impossible work budget. Each delete can reduce the minimum live
count by at most one. Discovery checks this lower bound before adding a
suffix key to its map. Full validation checks the base digest and charges
exact live-state work and payload, including intermediate batch states.
The authenticated final totals determine the exact new log length,
including its header and one record per live key. Restore checks that
length and the live-key count and bytes before sorting suffix changes or
creating the private output. The writer reserves the encoded bytes
again against the actual new log.
When the suffix is empty, the base frame can reject insufficient output
limits before the restore reads the base body. Admitted inputs still undergo
full authentication and exact admission. Restore checks destination availability before replay and again
before staging. An occupied file, directory or symlink at either check
returns `NotPublished(AlreadyExists)`. Atomic no-clobber publication
handles later conflicts under the existing conservative outcome rules.
Each value is authenticated again when read for the private output. Values
that fit in a write group are read directly into their record buffers and
checked before the record joins the group. Larger values use the separate
streamed record path. Before publication, a streaming verification of the
witnessed output checks the
restored sequence of keys, lengths and value digests. Restore retains
metadata for suffix-touched keys and uses separate batch and I/O buffers.

## Independent writable generations

`retained_candidate::RetainedStore` builds a new directory from a verified
candidate, optionally importing its independently bound detached suffix.
The destination is published without replacement after the staged base,
log and witness are synchronized and the staged generation reopens
successfully. The writable open reestablishes directory durability before
tail adoption or writes, including at a visible destination after an
uncertain creation. Source copies remain independently available.

The first 112 bytes of `records.log` contain the independently retained
candidate header beginning with `EMDALOG4`. The next eight bytes are
`EMDARW01`, followed by a fresh, nonzero 16-byte generation identifier.
Eleven little-endian u64 values record the admission ceilings in this order.
The four base limits are file bytes, entries, key bytes and verification
bytes. The two index limits are block count and anchor bytes. The three
suffix limits are encoded bytes, ordered operations and replay work. The
last two are overlay entries and overlay key bytes. DB01 and DB02 suffix
records begin after the 224-byte header. A reopening caller supplies the
independent base binding and the same limits.

The 84-byte `commit.state` starts with `EMDAWIT4`, followed by SHA-256 of
the header, the little-endian u64 committed offset relative to the start
of the suffix, SHA-256 of the committed suffix bytes, and CRC32 of the
preceding witness bytes. The writable open holds an exclusive store lock
and verifies the bound base, exact witnessed prefix, whole-record boundary,
and complete suffix within the persisted budgets. Recovery validates the
full base after directory synchronization and checks its path against the
verified descriptor before tail adoption or returning a handle. When
independent authority applies, a rejected suffix skips the full base scan.
A fully validated tail is adopted by synchronizing the log, streaming the
whole admitted suffix again to compare its digest with the replay digest,
and rechecking the
header, log length and saved witness before updating and synchronizing the
witness. A missing or malformed witness, changed suffix bytes, torn suffix,
split boundary or recovery limit failure prevents adoption. The extra pass
applies only when adoption is needed and reads at most the admitted suffix
size. File stability under the cooperative locking contract still applies.

Writable recovery streams physical records through a read buffer no
larger than 64 KiB or the admitted suffix. Buffered base verification and
copying also size their buffers to the admitted byte range. For a batch,
recovery retains at most 1,024 child keys and value
locations until the checksums and witness boundary pass. A batch over
the remaining operation budget or the minimum charged overlay work
avoids child allocation and decoding.
A single put over that budget skips the value digest used by the index.
The decoder still checks the record checksum and the acknowledged
digest before reporting admission failure. Once a batch is verified,
recovery charges its children in order and can merge its final edits in
place. A strictly ordered batch above the existing overlay extends it
directly after admission. Both paths move decoded keys without copying
their bytes. Recovery drops the read
buffer before opening the base. The overlay keeps the last value's
file offset, length and SHA-256 digest for each touched live key.
Point and page reads authenticate selected suffix values against
those digests. Tombstones and staged transaction edits participate in
the same ordered merge. Backup copies the suffix in bounded chunks
and checks the staged digest. Rebase streams the suffix to calculate
historical peaks and verifies the actual bytes it used. Writable import and managed
backup restore copy the admitted detached suffix from one opened file
in chunks of at most 1 MiB. A pinned base descriptor supplies the frame
checks before copying. The private suffix must match the exact endpoint and
independent digest, then pass operation, work and overlay admission through
writable replay before the engine copies the base. The prepared base is
still authenticated in full, and the private directory is synchronized
before publication. Recovery cannot adopt an added tail during initialization.

Standalone, managed and authority-backed opens can opt into
`open_with_suffix_scan_limit`. It charges one complete logical suffix
scan for replay and a second if the physical suffix extends beyond the
local witness. Both scans must fit before reading the suffix body or
authenticating the base. A rejected open leaves the witness, selection
and authority intent in place. Header, witness, base and catalog reads,
physical I/O retries, and reads after open are outside this per-open
ceiling. Catalog opens can apply a storage ceiling at the same time.

The same three open paths provide `open_with_recovery_scan_limit` to
cap the combined logical work of one complete encoded base scan and the
required suffix scans. Admission uses the bound base length and the
physical suffix length before reading either payload. One byte below
the required sum returns `InvalidInput`; an exact fit proceeds with full
authentication. This per-open budget also excludes the base framing and
EOF probes. Persisted generation and catalog limits still apply, and
authority-backed opens preserve any pending intent on budget rejection.

Mutations require exclusive use of the handle.
The encoder checks physical suffix bytes and ordered operation count before
allocating an encoded put, delete, or batch. With automatic rotation, the
precheck uses the limits of an empty suffix. The actual append still checks
the selected suffix, overlay and replay work. Accepted DB02 batches encode
their children and outer checksum directly into one fallibly reserved
buffer. Preparation
accounts for every child in replay order and reserves overlay growth
and changed keys before the append. Commit appends the
complete record, synchronizes the log, updates and synchronizes the
witness, then publishes changes in memory. When many new keys would
repeatedly shift the same overlay entries, publication uses a reverse
merge in the reserved vector. Admission still charges every operation
in replay order. An uncertain write poisons
the handle until reopening. The caps govern the file input and logical
retained state, while returned pages, encoded appends, staging,
allocator overhead and filesystem blocks have separate costs.
The detached inspection APIs that return materialized state have
their own staging limits. Inspection from a file and offline restore check
the base frame before reading the suffix body. They reject impossible
initial replay work, total staging bytes, and per-value limits inferred from
the declared value bytes and entry count. Successful calls still verify
both inputs and charge the exact ordered replay work. No on-disk fields change.
File inspection uses verified value locations for replay and allocates
payload only for live rows. The returned bytes are checked against the
captured digests. Touched-key metadata, decode buffers and allocator
overhead are additional costs.
The admission pass uses value lengths for intermediate payload budgets.
It authenticates the full base while reserving per-value digests for
later passes that need them.
Offline restore and private suffix-export staging authenticate the suffix
while discovering touched keys. If discovery returns an error, a raw
digest check distinguishes changed bytes from a replay error.
`open_exported_suffix_with_limits` and `restore_exported_suffix_with_limits`
accept independent ceilings on the number of distinct suffix keys and
their total key bytes. Discovery charges each key once, even if its last
mutation is a tombstone or the key was never live. It checks the ceilings
before adding the key to the map. Restore rejects a limit breach before
creating a destination stage. The original entry points derive their
ceilings from the existing suffix byte and operation limits. These are
logical bounds and exclude map and sort overhead, batch scratch and
returned values.
Suffix export uses the independently bound base frame to select the historic
source boundary, then authenticates the base while comparing its entries
with the reconstructed source prefix. Comparison results are used only
after the complete digest and historical peaks have been checked.

This is a separately created generation under the same trusted local
filesystem model. The local witness can detect mismatches and loss below
an intact boundary, while coordinated rollback of the generation and its
witness remains possible. Durable online generation selection, lifecycle
integration and retirement of prior history are covered by the remaining
design below.

## Managed retained catalogs

`retained_candidate::ManagedRetainedStore` creates a separate store root
from a verified base and optional detached suffix. It holds the catalog's
exclusive `store.lock` for the lifetime of the handle. One of two fixed
generation directories, `generation-a` or `generation-b`, is active.
Managed generation logs use `EMDARW02` at bytes 112–119 and append the
16-byte catalog identifier to the independent format's header. Their
240-byte header, including the catalog identifier, is bound by
`commit.state`. Independent retained open requires the `EMDARW01` role.

`catalog.id` is 56 bytes containing `EMDACT01`, the nonzero catalog ID,
and SHA-256 of those first 24 bytes. `active.retained` is a 256-byte
record containing `EMDAAC01`, the catalog ID, a one-byte slot selector
`0` or `1`, seven zero bytes, the generation ID, the 176-byte candidate
binding, and SHA-256 of the preceding 224 bytes. Open checks the marker
and exact generation and synchronizes the directory ancestry before
adopting a tail or accepting writes. Write admission rechecks the catalog
and active selection. Invalid or missing selection stops recovery.

Compaction verifies the actual current base and suffix, replays ordered
suffix updates for the historical peaks, and streams verified pages to a
fresh base with the same live state. The inactive slot must be empty.
The prepared generation has an empty suffix and is synchronized and
verified before an atomic replacement of `active.retained`. Writes are
admitted on the replacement only after directory synchronization.
Failure before selection can leave an inactive prepared generation.
A failure during selection disables the handle until reopening, which
recovers only the checked selection actually present. Recovery never
guesses a replacement selection from directory contents.

Managed open accepts a recovery ceiling for each resource and checks the
witness-bound persisted limits before loading the generation. The persisted
values, available through `limits`, govern writes and ordinary compaction.
Before reading record payloads, the opener checks the physical base length
against the expected binding and the admitted byte and work limits. Before
replaying the suffix, it also checks the base header, declared counts and
length, and the trailer against the admitted limits and independent binding.
The opener rejects impossible key and value totals and uses the declared
lengths to reject sparse-index budgets that cannot fit even the minimum
number of blocks and anchor bytes.
Base block-directory and suffix-overlay reservations grow geometrically
within the corresponding entry and key-byte ceilings. Each block requires
a nonempty anchor and each overlay entry requires a nonempty key, so either
ceiling can bound the number of requested slots. Allocator rounding, the
decoded record, and read and result buffers have separate costs.
A matching header and trailer still require full base validation, including
hashing the entry bytes and validating their order and lengths, before any
unacknowledged tail can be adopted or a handle returned.
`compact_with_limits` verifies the active generation under its existing
limits and publishes a successor with the explicitly requested values. This
allows compaction when a readable live set has outgrown the current base
budget, even if the suffix is full. After an uncertain resize, one recovery
ceiling must cover every limit in both the previous and successor generations.

The managed and authority-backed retained stores offer `compact_detailed`
and detailed variants for changing generation or persistent storage limits.
`NotPublished` means this call attempted neither successor publication
nor selection. `SuccessorOutcomeUnknown` means the successor may occupy the
inactive slot and this call did not attempt selection. Inspect and reclaim
through the catalog before retrying. `SelectionOutcomeUnknown` covers a
local selection replacement or an independent authority compare-and-swap
attempt. Preserve both slots and reopen against the appropriate selection
metadata before reclaiming or retrying. The original compaction methods
continue to classify the selection attempt only.

Rebase checks the source frame, the base blocks that may contain
suffix-touched keys, and the entire witnessed suffix while reconstructing
historical peaks. It sizes the successor from the original encoded size,
authenticated lengths of replaced or deleted entries, and live suffix
locations. The sparse index identifies which blocks the sizing pass needs.
It checks the successor file and verification byte limits and lower bounds
on block count and anchor bytes before creating the private base file.
During the merge, it admits each block before writing the entry that would
start it. The writer streams values through fixed-size buffers and authenticates
all base blocks, including values shadowed by replacements or deletes. The
successor opens its synced base once to authenticate it against the writer's
digest and construct its sparse index. Rotation then rechecks the complete
source before publishing the successor. For a base already parsed at open,
the recheck hashes the original admitted range on the pinned descriptor
and checks the authenticated digest, trailer and exact EOF. The sizing
and writing paths allocate no scan pages or full value copies. The suffix,
overlay, sparse indexes and staged files have their own resource costs.

`reclaim_inactive` verifies the active bytes and synchronized selection,
checks the inactive slot's ownership and file types, and moves it under
an engine-owned stage marked with the replacement generation's identity.
Retirement writes and synchronizes its marker under a preparation name,
then publishes the complete marker by a no-replace rename and synchronizes
the catalog directory before creating the payload. Generic staging cleanup
can remove abandoned preparation files under its exclusive parent lock.
It skips published retirement markers. Retry through `reclaim_inactive`
or reopen with automatic reclamation. A retry verifies
the selected replacement and its named files before resuming ordered
removal. Retirement synchronizes the catalog after removing the stage
payload and again after removing its marker. It reports either sync
failure. The first failure leaves the marker for a retry. If the second
sync fails, the marker may reappear after a crash, and cleanup can resume
without the retired payload. The optional `reclaim_inactive_detailed`
reports whether this call attempted to move the inactive slot or remove
anything from a previously interrupted retirement. An authority-backed
call can leave the fence outcome uncertain before attempting any removal.
On an authorized catalog, `reclaim_inactive_with_outcome` separates the
fence and removal outcomes. The provider can leave the fence unconfirmed
even when no removal started. Reopen to reconcile an unconfirmed fence.
A confirmed fence can accompany either a pre-removal validation error or
uncertain removal. Cleanup resumed from an earlier call can start before
a new fence is attempted. The original reclamation methods retain their
removal-only classification.

Managed and authority-backed handles offer
`reclaim_inactive_with_scan_limit` for explicit retirement. Each active
generation verification charges the full encoded base plus live suffix
length against an inclusive per-call budget. This includes verification
before resuming a stage and a separate charge if there is also an inactive
slot to retire. When a charge does not fit, that verification and its
removal are skipped. A new authority fence is submitted only after its
verification has been admitted. A call that already cleaned an older stage
reports that removal attempt even if the next budget charge fails.
Metadata and framing checks, the EOF probe and physical I/O retries have
separate costs.

Resolve a conflicting or unrecognized retirement
marker before rotation. Generic stages left by older releases have no replacement
binding and retain the earlier cleanup contract. Compaction and reclaim require
exclusive use of the handle. No public reader or writer can pin the
inactive slot outside that catalog. The requested successor limits can
reject a rebase before changing the selection. The two slots, temporary
stages, directory entries, returned pages and allocator overhead have
separate storage and memory costs.

Managed and authority-backed handles can use `compact_with_scan_limit`
for explicit rotation under an inclusive logical read ceiling. It charges
the selected base blocks used for sizing, the full base body streamed
during the merge, both suffix passes and the live suffix values used by
the merge, the full final source-base verification, and the successor's
complete base readback. The successor cost is admitted before writing the
replacement base. A budget refusal reports `NotPublished(InvalidInput)`
before publishing the successor or attempting selection. The ceiling
uses the current persisted generation limits and applies only to that
rotation. Separate framing and EOF probes, metadata, physical retries,
and all writes have separate costs.

`open_with_auto_reclaim` opts one handle into synchronous maintenance. On
open it clears abandoned private stages within the catalog and retires any
verified inactive generation. Before writing to a nonempty suffix, it rotates
if the projected byte count crosses the configured threshold or if the
append exhausts a persisted suffix or overlay limit. The triggering mutation
must fit in an empty suffix. Maintenance selects the new generation and
retires the previous one before submitting the mutation, so a maintenance
failure rejects that mutation even if the generation switched. An uncertain
selection still requires reopening. An oversized live set may require a
manual `compact_with_limits` before automatic maintenance can proceed.
The threshold is a maintenance trigger. A single record in an empty suffix
can exceed it while staying within the persisted limits. Maintenance runs
on the caller's write path and temporarily uses both slots.

To bound automatic maintenance, call
`enable_auto_reclaim_with_scan_limit` on a recovered managed or authorized
handle. This first runs cleanup under the supplied ceiling, then installs
the policy for that handle. Each mutation gets a fresh ceiling shared by
all retirement verifications and the rotation it triggers. The logical
charges are the same as for the explicit operations above. A budget
rejection can follow a completed generation switch or an earlier cleanup,
but it occurs before the requested mutation is appended. A later call
can spend its own ceiling to finish pending cleanup. The ceiling applies
to the maintenance scans. Initial recovery, appends, metadata, and generic
staging cleanup have separate costs. The existing automatic open methods
continue to install maintenance without a scan ceiling.

`open_with_bounded_startup` on either handle admits recovery and startup
retirement under one inclusive logical read ceiling. Recovery charges the
encoded base and suffix, with a second suffix pass for local witness
adoption. If an inactive slot or retained retirement markers are present,
admission reserves one full active-generation verification pass for each
condition. Admission precedes replay, local witness adoption, authority
resolution, and cleanup. The reservation also applies when an entry later
fails retirement validation. Once admitted, recovery can finish before a
subsequent cleanup error. Each later mutation has a separate ceiling for
rotation and retirement. Metadata, framing probes, physical retries, and
writes have separate costs.

### Whole-catalog storage admission

`open_with_storage_limits` on the managed and authorized handles sets
per-open limits on logical regular-file bytes and directory entries.
An authorized original-path handle measures the entire original
directory, including the fence, archives, failed cutover preparations,
and staging files. Place the independent authority and backups outside
that directory. For a separate catalog the measured tree is its catalog
directory. Hard links are charged for each directory entry. The walk
rejects symbolic links and special files and stops at the supplied cap.
It opens one directory iterator at a time and charges pending directory
paths against the entry budget. The logical quotas do not cap the
process memory used by those paths.

The live lock spans admission and mutation. The open checks existing
usage before adopting a tail or finalizing an authority intent. Appends
reserve their full encoded length before any append or authority update.
Rotation reserves its successor's log header, witness, base, and staging
metadata, including the managed selection replacement, while the active
generation still exists. The base writer fits its output to the remaining
catalog budget and its own persisted generation limits. Reclamation
reserves room for its private staging marker and directory. Automatic
rotation and every rotation carrying a persistent quota account for
that headroom before selecting the successor. While the inactive slot
exists, appends under a persistent quota preserve room for the retirement
marker and directory. Retire the inactive slot to release those bytes
for appends. Stale staging artifacts consume budget until cleanup.
A catalog already full under an earlier persisted policy can still need
a verified backup and restore into a separate catalog with more headroom.

`compact_with_persisted_storage_limits` publishes a successor whose
witness-bound header carries both catalog ceilings (`EMDARW04` for
managed selection and `EMDARW05` for authority selection). The authority
also binds the selected header digest. Preparation must fit the current
and requested ceilings. After selection, every supported open checks
the selected policy before suffix replay or tail adoption. An additional
per-open ceiling can only tighten it. Ordinary rotation carries the
selected policy forward. After an uncertain selection, reopen resolves
which generation and policy apply. Readers that lack the new header
version reject a quota-bearing selection.

Use the creation, copy, preparation, cutover and restore variants ending
in `_with_storage_limits` to publish the first generation with a quota.
They reserve catalog metadata before writing the base. Detached imports
reserve the bound suffix length. Original-path cutover also charges the
existing tree, archive hard links, the new catalog and fence staging at
their peak before the original log is replaced. Separate catalogs count
their contents. The ownership marker and directory entry in the parent
are outside a separate catalog's tree. Supply a policy explicitly when
restoring a backup into a new catalog.

Leave room for reclamation and future rotation when setting a ceiling.
These limits account for logical bytes and entries in trusted, stable
directories. Physical filesystem allocation, external authority files,
backups and other catalog roots have separate policies.

Catalogs use trusted local directories and the ordinary rollback model.
The original ordinary store remains independently addressable after
import. The catalog's safe retirement applies to its own inactive slots.

`ManagedRetainedStore::copy_from_store` takes ownership of a witnessed
ordinary store and holds its lock through publication at a separate path.
It checks source identity and destination separation before staging.
It replays the full witnessed history within the export limits. The sizing
pass checks live entry and key bytes and computes the exact block and anchor
requirements before creating the base file. This applies to both import
and cutover. The engine then verifies the compacted base and initializes an
empty witnessed suffix under the managed generation limits. Base export
drains a fixed write buffer before syncing and re-reading the staged file.
A drain, sync, validation or source-witness failure leaves the stage
unpublished. The base carries the original ordered batch peaks and stays
inside private staging until the complete catalog is published. The source remains an independently writable store
after the copy. Applications must coordinate any move of their writers to
the new path. An uncertain publication can be inspected by opening the
destination and verifying the selected generation.

`backup_to` captures the active catalog's verified base and exact witnessed
suffix in a new directory. It copies from the open generation descriptors
using independent read positions, then syncs and checks the staged files
against the captured binding. Source base identity, log length, witness and
catalog selection are checked through publication preparation. The caller
must retain the returned binding
independently, including when publication returns an uncertain outcome.
`restore_backup` checks that binding and the supplied resource ceilings
before publishing a fresh writable catalog. Backups retain their snapshot
after source compaction and reclamation. Restored catalogs and their sources
can be opened independently, so moving writers still requires application
coordination. A trusted binding identifies the retained snapshot. Freshness
after later source writes requires an independent freshness policy.

`backup_to_with_scan_limit` on both catalog types charges the complete
encoded base and suffix twice. The first pass copies the source and the
second verifies the staged backup. Managed `restore_backup_with_scan_limit`
and authorized `prepare_from_backup_recorded_with_scan_limit` likewise
charge the bound byte lengths for the copy and staged verification or
replay. Admission precedes staging and receipt recording. An optional
persistent storage quota applies separately. The logical ceilings cover
payload reads. Metadata and framing probes, physical retries and writes
have separate costs.

## Future migration of existing stores and external authority

`AuthorizedRetainedStore` exercises independent authority on a newly
prepared catalog at a separate path. Its catalog and generation roles
require the authorized opener. The caller registers the verified initial
record with a trusted provider using CAS from an absent record. A provider
must supply durable linearizable compare-and-swap and independently protect
the lineage from rollback or reuse.

`FileRetainedAuthority` provides durable compare-and-swap for cooperating
processes on one host. Its provisioned directory holds a permanent lock
and a checksummed, fixed-size record. Creation writes an explicit empty
record. Missing or damaged records on open fail closed. Under an exclusive
file lock, updates check the full prior record and next revision, sync a
complete replacement, rename it atomically, and sync the authority
directory before returning success. The reserved `.emda-authority-next-v1`
file is private to this protocol. It can be removed under the exclusive
lock after validating the published record, including after an interrupted
update. A nonregular or multiply linked entry at that name fails closed
without being removed. Readers obtain the published record from
`authority.record`. Reads synchronize the visible record's
directory when observing a replacement. Each provider retains one descriptor
for its last validated, synchronized record. Under the same lock, a read
checks the complete record again and can reuse that synchronization for
the same open inode and contents. A different inode or failed sync requires
successful synchronization before the provider returns the record.

The deployment must put the authority directory on independently protected
storage and keep its contents and ancestors trusted and stable. A local
authority can protect against rollback of catalog files alone. Restoring
both the catalog and its authority together can defeat that protection.
The provider uses a blocking file lock and synchronous I/O.

An append first reserves a fresh authority revision naming the exact prior
and proposed suffix fingerprints. The local log and witness synchronize
before a second CAS finalizes the proposal and allows acknowledgement.
Before replay, reopen admits only the physical suffix length named by the
committed point or the pending proposal. It verifies the entire admitted
suffix and checks its digest against the exact authority point. When an
intent remains pending, it synchronizes or adopts the matching state before
finalizing the recovery choice. A successful local tail adoption already
synchronizes the pinned log and witness. If the witness was already
visible on open, recovery synchronizes both files before the final CAS.
An unrelated or partial suffix fails
closed. A stale copy can lose the CAS and must reopen against the current
record. Revisions advance even when recovery selects the prior suffix.

Authorized compaction writes the successor into the empty slot under
the exclusive catalog lock. It synchronizes and verifies the successor
before a single authority CAS selects its generation and empty suffix.
Reopen uses that record to find and verify the exact generation. A local
selector cannot revert an authority choice. A missing or corrupt selected
generation fails closed. An uncertain selection retains both slots.
Reclamation advances the authority revision without changing its selected
point before removing the inactive slot. Earlier in-flight selection
attempts lose their CAS, and a fresh rotation must build a fresh successor.
The ordinary authorized open leaves reclamation explicit.

`AuthorizedRetainedStore::open_with_auto_reclaim` opts into synchronous
maintenance for that handle. Open verifies and recovers the authority
selection, then removes owned abandoned stages and reclaims a verified
inactive slot. Writes rotate when the nonempty suffix would cross the
threshold or exceed a hard suffix or overlay limit. The proposed record
must first pass admission against an empty suffix. Rotation and inactive
reclamation use their own authority updates before the append submits its
intent. The move to private staging synchronizes both parent directories
before removing the retired files. Automatic cleanup on reopen
synchronizes the catalog directory before removing abandoned stages.

Successful maintenance may remain visible when the following append is
rejected or cancelled. An uncertain maintenance update requires reopen
and reports the user write as rejected because no append was attempted.
After the append begins, its original outcome rules apply. The option
adds synchronous work to writes and does not establish a latency,
whole-database disk or process-memory bound.

An opt-in blocking cutover can consume an exclusively owned witnessed
ordinary store at its original path. It exports into a private nested
catalog and preserves the previous records and witness under non-store
names. After synchronizing the verified candidate it replaces the
original `records.log` atomically with a checksummed `EMDALOG4` fence.
The fence names the nested catalog and contains its initial authority
record. The original store lock remains held through synchronization.

Register the returned initial record on absent independent authority.
If the publication outcome is uncertain, `recover_cutover` verifies the
visible fence and initial generation before supplying that record.
`recover_cutover_with_recovery_scan_limit` applies the combined base and
suffix recovery ceiling before reading those payloads.
If the ordinary store still opens, its data can be migrated again with a
fresh preparation. A failure before fencing can leave an inert nested
preparation. Authorized
open holds the original path's lock, checks the fence and authority
together, and opens the nested catalog. The nested catalog refuses direct
open. The ordinary entry point rejects the marker.

Before cutover, a caller can record the ordinary log length and SHA-256
of the log and witness file while it has exclusive use of the source.
Preserve those values independently as a `CutoverArchiveBinding`.
On V3 the witness-file digest covers the identity anchor. Archive
retirement works with the old catalog layout and does not require the
initial retained generation to still exist.

The authorized handle can publish a bound backup of the selected
generation at an outside path. `prepare_from_backup` and
`prepare_from_backup_with_storage_limits` verify an independently retained
backup binding under the supplied recovery limits and publish a new
authorized catalog and generation at an absent destination. Keep the
backup and destination directories trusted and stable. A successful or
uncertain publication returns the verified initial authority record. Save
it independently of the destination and pass it to `register_prepared`
with a fresh provider outside the catalog. Registration verifies and
synchronizes the destination under its catalog lock before the
compare-and-swap from an absent record. A retry can use the same initial
record after a lost provider response. Missing, mismatched or damaged
destinations and different authority records cannot be registered by
this path.

Use `prepare_from_backup_recorded` to close the interval between publishing
the destination and receiving the record. Its callback runs after staged
verification and synchronization and before the no-clobber rename. Persist
the supplied record as a separate receipt and return success only after
the receipt is durable. Use a fresh authority for registration. If the
callback fails, publication does not begin. After interruption, the receipt
can exist with an absent destination. `register_prepared` checks the exact
visible catalog before advancing its authority, including after an
interrupted rename or directory sync.

`register_prepared_with_recovery_scan_limit` charges one complete base
verification and suffix replay, plus confirmation of the suffix when the
local witness is older than the prepared endpoint. A scan budget refusal
comes before witness adoption or an authority comparison.

This restores a selected snapshot into a new lineage. Keep the original
authority and choose the recovery point independently. The backup binding
identifies exact bytes and does not establish freshness. A failed restore
can leave a private stage. Prepare a new restore at another absent
destination if the independently held record was lost.

`retire_cutover_archive` requires a
backup of its exact current base and suffix. Retirement synchronizes the
opened backup files and their directory ancestry, verifies the exact
bytes on those descriptors, and confirms that the paths still name the
verified files before removing matching archive names. It also verifies
the permanent fence, the source root's witness file and the independent
selection. The backup must remain in trusted, stable directories throughout
the operation. Retirement keeps five backup file descriptors open through the
pre-unlink check.
Retirement preserves the original-path fence, root witness, active
generation and authority record. It does not remove independently
retained ordinary copies or guarantee saved blocks if other links
to the archive exist.

An unlink or parent sync error reports `OutcomeUnknown` once removal
begins. A retry verifies each remaining archive file against the
original binding and synchronizes the catalog directory. The backup
must still match the current selected generation at retry time.
`retire_cutover_archive_detailed` distinguishes verification failure,
an unconfirmed log or witness unlink, and catalog sync failure. Its
flags record unlink calls that returned success during that attempt.
They describe the observed operations. Persistence requires successful
catalog sync. Even a retry that finds neither archive name must
synchronize the catalog directory before returning success.

`retire_cutover_archive_with_scan_limit` applies an inclusive logical read
ceiling before payload verification or unlinks. The charge includes the
original root witness, each still-present archive file, and the complete
base and suffix of both the active and backup copies. A retry only charges
archive files still present and performs the same verification and sync.
A budget refusal reports the verification step with both unlink flags false.

An operator can discard a complete abandoned preparation by passing
the 16-byte identifier encoded in its `retained-cutover-*` name.
`discard_unfenced_preparation` consumes an open ordinary source and
checks its exclusive lock, both source hard links, and an absent
record in the independent authority intended for this cutover.
Coordinate discard with any process that could still register an
earlier cutover record.
An authorized original-path handle can use
`discard_other_cutover_preparation` after a successful retry. It checks
the current fence and authority, the old witness hard link, and the
archive fingerprints recorded for the successful cutover. This works
after the active catalog's ordinary archive is retired.

Cleanup checks the exact catalog layout and verifies the unwritten
initial generation within the supplied recovery ceiling. Unknown
entries and catalogs referenced by the current fence or authority
are protected. The original-path state is synchronized before moving
the preparation into a private stage. Both parents are synchronized
before its contents are removed. On uncertain removal, inspect the
original name and retry as needed. Abandoned private stages use the
regular staging cleanup procedure. The directories and the authority
must remain trusted and stable throughout the operation.

Deployment still requires an inventory and fencing of older backups and
independent writable copies. Restoring an entire ordinary snapshot can
remove the local marker from the view of an old executable. Authorized
open verifies the independent authority and fails closed in that case.

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
backup guarantees; and process/fork lock ownership. Production cutover
requires verification of the deployment's supported readers and backup policy.

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

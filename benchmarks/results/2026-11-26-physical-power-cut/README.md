# Physical power cut during acknowledged atomic writes

One physical power cut, on one hardware and filesystem configuration, while
an ordinary `Store` acknowledged atomic batches. Revision `45e457c6`.

## Configuration

- Raspberry Pi 5 Model B Rev 1.1, 8 GB, aarch64 Linux.
- Samsung MZVLQ1T0HBLB-00B 1 TB NVMe, ext4 mounted `rw,noatime` on
  `/dev/nvme0n1p2`.
- `power_cut_probe` was a temporary example built in a disposable source
  copy at the revision above. It is archived here and was not added to the
  repository. The first build failed on a wrong import (`pi-build.log`); the
  corrected probe built on retry (`pi-build-retry.log`). The binary's
  SHA-256 is in `preflight.log`.
- The store used the default `Store::open` format for that filesystem. The
  log header was not separately recorded.

## Workload

Each iteration commits one `Store::write_batch` of two puts, `a/<id>` and
`b/<id>`, with distinct 4 KiB values, and prints `ACK <id>` only after the
call returns. A batch is synchronized before it is acknowledged. The probe
pauses 35 ms between batches.

On a Mac, `observe.py` ran the writer over SSH. It accepted only the next
consecutive `ACK <id>`, and appended and fsynced each receipt to a local file
before reading the next. A receipt on the Mac therefore means the batch had
returned on the Pi.

## Operator action

The owner, authorized and present at the device, cut the power while writes
were running. The cut was not repeated.

## Machine observations

- The Mac recorded 497 receipts, `ACK 1` to `ACK 497`, consecutive and
  without gaps (`cut-acks.log`). The SSH session then timed out
  (`writer.log`, exit 255), and the host was unreachable from 17:44:39Z to
  17:45:24Z UTC (`ssh-unreachable.log`).
- The kernel boot ID changed from `37d04a54-d0b1-4379-9958-2816ae2b0546`
  to `5347eb18-7451-47d1-b572-47c8c22550e9`, confirming a reboot.
- After restart, an ordinary `Store::open` recovered the store. The probe
  verified all 497 acknowledged batches, 994 values, byte for byte
  (`result.log`). No acknowledged value was missing or altered.
- Postflight, four sampled services were active, `systemctl --failed`
  listed no units, and the same NVMe device was present (`postflight.log`).

A 12-batch smoke run before the cut acknowledged normally
(`smoke-acks.log`).

## What this supports

On this configuration, for this live workload and one cut, every
acknowledged atomic batch survived a physical power loss and was recovered
by a normal open. It does not establish:

- behavior on other hardware, filesystems, mount options or write caches;
- behavior at other boundaries, such as maintenance, checkpoints,
  rotation, backup or compaction;
- whether unacknowledged in-flight batches survived, which was not checked;
- any statistical bound from more cuts.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the revision, the preflight and postflight records,
both boot IDs, the reachability log, the Mac observer and its receipts, the
smoke receipts, the result, the probe source, the build logs and
`SHA256SUMS`. Private network addresses and home paths are redacted.

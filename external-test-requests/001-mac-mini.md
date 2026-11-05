# Request 001: Mac mini setup

The existing comparison runner uses Linux-specific process affinity, `/proc`,
and Docker socket behavior, so it cannot run directly on macOS. Use an ARM64
Linux VM on the Mac for this request. The prototype, SQLite, PostgreSQL and
MongoDB then run in the same guest and on the same guest filesystem. The
pinned container images include ARM64 variants; do not enable Rosetta or x86
emulation for this run. The resulting numbers include VM and virtual-storage
effects and are **not** native macOS results or proof of physical power-loss
behavior. Native macOS compatibility can be investigated separately.

The benchmark remains pinned to
`0edfebaeb0be8693813a66e4740e7537127ab2db`, even if development continues and
`main` changes while it runs. Use the latest instructions and the pinned code
revision specified in the main [request](001-compound-range-benchmark.md).

## Create the VM

On the Mac, install [Homebrew](https://brew.sh/) if needed, then Lima 2.x with
`brew install lima`. The following commands use a new, disposable Ubuntu 24.04
VM with Apple's virtualization framework, four virtual CPUs, 8 GiB RAM and a
40 GiB virtual disk. Adjust these settings to fit the host; leave enough memory
and CPU available to macOS. The virtual disk consumes host storage as it
grows. The machine must have internet access to install the VM, Rust,
dependencies and container images.

Choose a new directory and VM name if either already exists. The commands
clone the public repository separately from any existing checkout. The VM
template installs Docker and build dependencies **inside the VM**, adds its
guest user to the Docker group, and mounts no host directories. Docker group
membership provides root-equivalent access within this disposable guest.

```bash
export EMDA_MAC_WORK="$HOME/emda-mac-benchmark-001"
git clone https://github.com/Eta06/EMDA-DB.git "$EMDA_MAC_WORK"
limactl start --yes --name=emda-db-test-001 --vm-type=vz --arch=aarch64 \
  --cpus=4 --memory=8 --disk=40 --mount-none \
  "$EMDA_MAC_WORK/external-test-requests/mac-mini-linux-vm.yaml"
```

Create a unique results directory on the Mac and record non-secret host
details. Do not include the Mac's serial number or UUID. Use a fresh directory
for each repeat of the whole experiment.

```bash
export EMDA_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export EMDA_MAC_RESULTS="$EMDA_MAC_WORK/external-test-requests/results/001-mac-mini/$EMDA_RUN_ID"
mkdir -p "$EMDA_MAC_RESULTS"
{
  sw_vers
  printf 'Host architecture: '; uname -m
  printf 'CPU: '; sysctl -n machdep.cpu.brand_string
  printf 'Logical CPUs: '; sysctl -n hw.ncpu
  printf 'Memory bytes: '; sysctl -n hw.memsize
  printf 'Lima version: '; limactl --version
} > "$EMDA_MAC_RESULTS/mac-host.txt"
```

## Run inside the VM

Open a guest Bash session:

```bash
limactl shell --workdir=/var/tmp emda-db-test-001 -- /bin/bash
```

Inside that session, install Rust with the official installer, then select a
test parent on the VM's virtual disk. `findmnt` should show a guest filesystem,
not a host-shared mount or tmpfs. If Docker, Rust, an image, or MongoDB cannot
start, keep the error and report it; do not disable its checks.

```bash
uname -m
docker --host=unix:///var/run/docker.sock version
findmnt -T /var/tmp -o TARGET,FSTYPE,SOURCE,OPTIONS
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/emda-rustup-init.sh
sh /tmp/emda-rustup-init.sh -y --profile minimal --default-toolchain stable
. "$HOME/.cargo/env"
export EMDA_TEST_PARENT=/var/tmp/emda-benchmark-001
```

Now run all three Bash blocks in the [main request](001-compound-range-benchmark.md)
**in this same guest session**. Its first block preserves the `EMDA_TEST_PARENT`
set above, checks out the pinned revision into a separate directory, and
prints the generated `EMDA_TEST_DIR`. Its smoke and two full runs write their
JSON files there. After the runs, still inside the guest, record the path:

```bash
printf '%s\n' "$EMDA_TEST_DIR" > /var/tmp/emda-request-001-run-path.txt
exit
```

If a command failed before the path was recorded, the guest's
`/var/tmp/emda-benchmark-001/` contains the created `emda-range.*` directory;
retain any partial files and error output instead of replacing them.

## Return the results

Back on the Mac, in the same shell that set `EMDA_MAC_RESULTS`, copy the
files from the guest. `limactl copy` uses the VM connection and does not
require a shared mount.

```bash
EMDA_GUEST_DIR="$(limactl shell --workdir=/var/tmp emda-db-test-001 -- cat /var/tmp/emda-request-001-run-path.txt)"
for EMDA_FILE in smoke.json primary.json repeat.json host.txt; do
  limactl copy "emda-db-test-001:$EMDA_GUEST_DIR/$EMDA_FILE" "$EMDA_MAC_RESULTS/"
done
printf 'Results: %s\n' "$EMDA_MAC_RESULTS"
```

Add a `notes.md` describing the VM CPU/RAM/disk allocation, storage type,
whether other workloads were active, failures and any redactions. Review the
files for private paths or machine details, then add the directory under
[`results/001-mac-mini/`](results/001-mac-mini/README.md) to a branch or
commit, or share it in chat. Tell us where to find it when ready; ongoing work
does not wait for the results.

The VM can be stopped with `limactl stop emda-db-test-001` after the files
have been copied. Keep the VM and original results until they have been
reviewed; stopping it does not delete them.

#!/usr/bin/env bash
# Requires a disposable Linux host with ext4, loop devices, OpenSSL and sudo.
set -euo pipefail
binary=$(realpath "${1:?pass the Linux emda-server binary}")
[[ -x "$binary" ]]
scratch=$(mktemp -d /tmp/emda-physical-quota.XXXXXXXX)
alpha="$scratch/projects/alpha"
server_pid=''
cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" || true
    fi
    if mountpoint -q "$alpha"; then
        sudo -n umount "$alpha" || {
            echo "Quota test mount could not be released at $alpha" >&2
            return 1
        }
    fi
    rm -rf -- "$scratch"
}
trap cleanup EXIT
umask 077
mkdir -p "$alpha"
truncate -s 24M "$scratch/alpha.img"
mkfs.ext4 -q -F -m 0 "$scratch/alpha.img"
sudo -n mount -o loop,nodev,nosuid,noexec "$scratch/alpha.img" "$alpha"
sudo -n chown "$(id -u):$(id -g)" "$alpha"
chmod 0700 "$alpha"
# ext4 creates this empty directory when initializing a new test volume.
sudo -n rmdir "$alpha/lost+found"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
    -addext subjectAltName=DNS:localhost -keyout "$scratch/key.pem" \
    -out "$scratch/cert.pem" 2>/dev/null
python3 - "$scratch" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
projects = []
for project, byte in [("alpha", 1), ("beta", 2)]:
    projects.append({
        "id": project, "data_log_bytes": 64 * 1024 * 1024, "sql_log_bytes": 64 * 1024 * 1024,
        "max_live_keys_per_store": 128, "max_in_flight": 2,
        "credentials": [{
            "id": "operator", "role": "admin", "expires_unix_seconds": 4102444800,
            "token_sha256": hashlib.sha256(bytes([byte]) * 32).hexdigest(),
        }],
    })
projects[0]["max_volume_bytes"] = 24 * 1024 * 1024
config = {
    "bind": "127.0.0.1:0", "data_root": str(root / "projects"),
    "tls_cert": str(root / "cert.pem"), "tls_key": str(root / "key.pem"),
    "require_hybrid_kex": False, "max_connections": 8, "timeout_ms": 5000, "projects": projects,
}
(root / "server.json").write_text(json.dumps(config))
projects[1]["max_volume_bytes"] = 24 * 1024 * 1024
(root / "bad-volume.json").write_text(json.dumps(config))
del projects[1]["max_volume_bytes"]
projects[0]["max_volume_bytes"] = 4 * 1024 * 1024
(root / "oversized-volume.json").write_text(json.dumps(config))
PY

reject_config() {
    local config=$1 expected=$2 status=0
    timeout 5 "$binary" "$config" >"$scratch/invalid.out" 2>&1 || status=$?
    if [[ "$status" != 1 ]] || ! grep -Fq "$expected" "$scratch/invalid.out"; then
        cat "$scratch/invalid.out" >&2
        echo "Expected startup refusal for $config" >&2
        return 1
    fi
}

mkdir -m 0700 "$scratch/projects/beta"
reject_config "$scratch/bad-volume.json" "bounded projects require a dedicated filesystem"
reject_config "$scratch/oversized-volume.json" "project volume exceeds max_volume_bytes"
[[ ! -e "$alpha/data/records.log" ]]

"$binary" "$scratch/server.json" >"$scratch/server.out" 2>"$scratch/server.err" &
server_pid=$!
port=''
for ((attempt=0; attempt<100; attempt++)); do
    port=$(sed -n 's/^READY 127\.0\.0\.1:\([0-9]*\)$/\1/p' "$scratch/server.out" | tail -1)
    [[ -z "$port" ]] || break
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 0.05
done
if [[ -z "$port" ]]; then
    cat "$scratch/server.err" >&2
    exit 1
fi

python3 - "$scratch" "$port" <<'PY'
import json
import os
import pathlib
import socket
import ssl
import struct
import sys

root = pathlib.Path(sys.argv[1])
port = int(sys.argv[2])
ctx = ssl.create_default_context(cafile=str(root / "cert.pem"))
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
alpha = root / "projects" / "alpha"
beta = root / "projects" / "beta"
assert alpha.stat().st_dev != beta.stat().st_dev

def send(project, operation):
    request = json.dumps({
        "version": 1, "project": project, "credential": "operator",
        "token": ("01" if project == "alpha" else "02") * 32,
        "operation": operation,
    }).encode()
    with socket.create_connection(("127.0.0.1", port), timeout=6) as tcp:
        with ctx.wrap_socket(tcp, server_hostname="localhost") as conn:
            conn.sendall(struct.pack("!I", len(request)) + request)
            with conn.makefile("rb") as response:
                size, = struct.unpack("!I", response.read(4))
                assert 0 < size <= 256 * 1024
                return json.loads(response.read(size))

get = {"op": "kv_get", "key": "witness"}
put = {"op": "kv_put", "key": "witness", "value_hex": "abcd"}
assert send("alpha", put) == {"ok": True, "result": None}
assert send("alpha", get) == {"ok": True, "result": "abcd"}
assert send("beta", put) == {"ok": True, "result": None}

chunk = {"op": "kv_put", "key": "fill", "value_hex": "aa" * (96 * 1024)}
for acknowledged in range(300):
    reply = send("alpha", chunk)
    if reply["ok"] is False:
        break
else:
    raise AssertionError("project volume accepted more data than its hard capacity")

# The engine log allowance is larger than the device. The host filesystem,
# rather than the logical log cap or the wire frame, rejected the write.
fs = os.statvfs(alpha)
assert acknowledged > 0
assert fs.f_bavail * fs.f_frsize < 1024 * 1024, (reply, fs)
assert (alpha / "data" / "records.log").stat().st_size < 64 * 1024 * 1024
assert reply["error"] in {"outcome_unknown", "limit"}, reply
assert send("beta", get) == {"ok": True, "result": "abcd"}
assert send("beta", {**put, "value_hex": "beef"}) == {"ok": True, "result": None}
assert send("beta", get) == {"ok": True, "result": "beef"}
print(f"Physical capacity isolated after {acknowledged} acknowledged writes; failing result={reply['error']}")
PY

kill -TERM "$server_pid"
wait "$server_pid"
server_pid=''
sudo -n umount "$alpha"
reject_config "$scratch/server.json" "bounded projects require a dedicated filesystem"
[[ ! -e "$alpha/data/records.log" ]]
printf 'Missing project mount rejected before creating a fallback log\n'

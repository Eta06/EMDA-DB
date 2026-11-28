#!/usr/bin/env bash
# Run on a disposable Linux host with systemd, cgroup v2, OpenSSL and sudo.
set -Eeuo pipefail
trap 'printf "Isolation verification failed at line %s\n" "$LINENO" >&2' ERR

binary=$(realpath "${1:?pass the Linux emda-server binary}")
[[ -x "$binary" ]]
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
scratch=$(sudo -n mktemp -d /opt/emda-isolation.XXXXXXXX)
suffix=$(printf "%s" "${scratch##*.}" | tr "[:upper:]" "[:lower:]")
template="emda-isolation-$suffix"
alpha="alpha-$suffix"
beta="beta-$suffix"
alpha_unit="$template@$alpha.service"
beta_unit="$template@$beta.service"
unit_file="/run/systemd/system/$template@.service"
cleanup() {
    sudo -n systemctl stop "$alpha_unit" "$beta_unit" || true
    sudo -n systemctl clean --what=state "$alpha_unit" "$beta_unit" || true
    sudo -n rm -f "$unit_file"
    sudo -n systemctl daemon-reload
    sudo -n rm -rf -- "$scratch"
}
trap cleanup EXIT

sudo -n chmod 0755 "$scratch"
sudo -n install -m 0755 "$binary" "$scratch/emda-server"
sed -e "s|/usr/local/bin/emda-server|$scratch/emda-server|" \
    -e "s|/etc/emda/projects/|$scratch/|" \
    "$repo/deploy/systemd/emda-server@.service" | sudo -n tee "$unit_file" >/dev/null

# Keep both stores and the generated TLS credentials in private directories.
for project in "$alpha" "$beta"; do
    sudo -n install -d -m 0700 "$scratch/$project"
    sudo -n install -d -m 0755 "$scratch/$project/live"
    sudo -n openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -subj /CN=localhost -addext subjectAltName=DNS:localhost \
        -keyout "$scratch/$project/tls-key.pem" -out "$scratch/$project/tls-cert.pem" 2>/dev/null
    sudo -n python3 - "$scratch" "$project" <<'PY'
import hashlib
import json
import os
import pathlib
import sys
scratch, project = sys.argv[1:]
test_token = bytes([1 if project.startswith("alpha-") else 2]) * 32
config = {
    "bind": "127.0.0.1:0",
    "data_root": "stores",
    "tls_cert": "tls-cert.pem",
    "tls_key": "tls-key.pem",
    "require_hybrid_kex": False,
    "max_connections": 8,
    "timeout_ms": 5000,
    "credentials_file": "/run/emda-live/credentials.json",
    "projects": [{
        "id": "app",
        "max_in_flight": 2,
        "data_log_bytes": 8192,
        "sql_log_bytes": 8192,
        "max_live_keys_per_store": 128,
    }],
}
auth = {"projects": [{"id": "app", "credentials": [{
    "id": "operator", "role": "admin", "expires_unix_seconds": 4102444800,
    "token_sha256": hashlib.sha256(test_token).hexdigest(),
}]}]}
project_dir = pathlib.Path(scratch) / project
os.umask(0o077)
(project_dir / "server.json").write_text(json.dumps(config))
policy = project_dir / "live" / "credentials.json"
policy.write_text(json.dumps(auth))
policy.chmod(0o644)
PY
done
sudo -n systemctl daemon-reload
if ! sudo -n systemctl start "$alpha_unit" "$beta_unit"; then
    sudo -n journalctl -q -u "$alpha_unit" -u "$beta_unit" -o cat --no-pager >&2
    exit 1
fi

wait_ready() {
    local unit=$1 port='' invocation=''
    for ((attempt=0; attempt<100; attempt++)); do
        invocation=$(sudo -n systemctl show -p InvocationID --value "$unit")
        port=$(sudo -n journalctl -q -u "$unit" "_SYSTEMD_INVOCATION_ID=$invocation" -o cat --no-pager | sed -n 's/^READY 127\.0\.0\.1:\([0-9]*\)$/\1/p' | tail -1)
        if [[ -n "$port" ]] && sudo -n systemctl is-active --quiet "$unit"; then
            printf '%s\n' "$port"
            return
        fi
        sleep 0.1
    done
    sudo -n journalctl -q -u "$unit" -o cat --no-pager >&2
    return 1
}

alpha_port=$(wait_ready "$alpha_unit")
beta_port=$(wait_ready "$beta_unit")
sudo -n python3 - "$scratch" "$alpha" "$alpha_port" "$beta" "$beta_port" "$alpha_unit" <<'PY'
import json
import pathlib
import socket
import ssl
import struct
import subprocess
import sys
import time
scratch, alpha, alpha_port, beta, beta_port, alpha_unit = sys.argv[1:]

def send(project, port, token, operation):
    ctx = ssl.create_default_context(cafile=str(pathlib.Path(scratch) / project / "tls-cert.pem"))
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    request = json.dumps({
        "version": 1, "project": "app", "credential": "operator", "token": token,
        "operation": operation,
    }).encode()
    with socket.create_connection(("127.0.0.1", int(port)), timeout=3) as tcp:
        with ctx.wrap_socket(tcp, server_hostname="localhost") as conn:
            conn.sendall(struct.pack("!I", len(request)) + request)
            with conn.makefile("rb") as response:
                length, = struct.unpack("!I", response.read(4))
                assert 0 < length <= 256 * 1024
                return json.loads(response.read(length))

key = {"op": "kv_get", "key": "probe"}
put = {"op": "kv_put", "key": "probe", "value_hex": "abcd"}
assert send(alpha, alpha_port, "01" * 32, put) == {"ok": True, "result": None}
assert send(beta, beta_port, "02" * 32, key) == {"ok": True, "result": None}
assert send(beta, beta_port, "01" * 32, key) == {"ok": False, "error": "unauthorized"}
assert send(alpha, alpha_port, "01" * 32, key) == {"ok": True, "result": "abcd"}
project_dir = pathlib.Path(scratch) / alpha
policy = project_dir / "live" / "credentials.json"
new_token = subprocess.run([
    str(pathlib.Path(scratch) / "emda-server"), "keys", str(project_dir / "server.json"),
    "--policy-file", str(policy), "rotate", "app", "operator", "4102444800",
], check=True, stdout=subprocess.PIPE, text=True).stdout.strip()
subprocess.run(["systemctl", "reload", alpha_unit], check=True)
deadline = time.monotonic() + 3
while send(alpha, alpha_port, new_token, key) != {"ok": True, "result": "abcd"}:
    assert time.monotonic() < deadline, "systemd reload did not publish the new key"
    time.sleep(0.02)
assert send(alpha, alpha_port, "01" * 32, key) == {"ok": False, "error": "unauthorized"}
assert send(beta, beta_port, "02" * 32, key) == {"ok": True, "result": None}
PY

alpha_pid=$(sudo -n systemctl show -p MainPID --value "$alpha_unit")
beta_pid=$(sudo -n systemctl show -p MainPID --value "$beta_unit")
alpha_uid=$(awk '$1 == "Uid:" {print $2}' "/proc/$alpha_pid/status")
beta_uid=$(awk '$1 == "Uid:" {print $2}' "/proc/$beta_pid/status")
alpha_gid=$(awk '$1 == "Gid:" {print $2}' "/proc/$alpha_pid/status")
beta_gid=$(awk '$1 == "Gid:" {print $2}' "/proc/$beta_pid/status")
if [[ "$alpha_uid" -le 0 || "$beta_uid" -le 0 || "$alpha_uid" == "$beta_uid" ]]; then
    printf 'Expected distinct unprivileged project users, got %s and %s\n' "$alpha_uid" "$beta_uid" >&2
    exit 1
fi
for unit in "$alpha_unit" "$beta_unit"; do
    cgroup=$(sudo -n systemctl show -p ControlGroup --value "$unit")
    [[ $(cat "/sys/fs/cgroup$cgroup/memory.max") == 536870912 ]]
    [[ $(cat "/sys/fs/cgroup$cgroup/pids.max") == 48 ]]
    read -r quota period <"/sys/fs/cgroup$cgroup/cpu.max"
    [[ "$quota" == "$period" && "$quota" != max ]]
done

alpha_store="/var/lib/emda-$alpha/stores/app/data/records.log"
beta_store="/var/lib/emda-$beta/stores/app/data/records.log"
for tuple in     "$alpha_pid:$alpha_uid:$alpha_gid:$alpha_store:$beta_store:$alpha_unit:$beta_unit"     "$beta_pid:$beta_uid:$beta_gid:$beta_store:$alpha_store:$beta_unit:$alpha_unit"; do
    IFS=: read -r pid uid gid own neighbor own_unit neighbor_unit <<<"$tuple"
    runner=(sudo -n nsenter --mount="/proc/$pid/ns/mnt" -- setpriv
        --reuid="$uid" --regid="$gid" --clear-groups /bin/test)
    "${runner[@]}" -r "$own"
    "${runner[@]}" -r /run/emda-live/credentials.json
    if "${runner[@]}" -w /run/emda-live/credentials.json; then
        echo "Project UID $uid can modify its operator-owned credentials" >&2
        exit 1
    fi
    "${runner[@]}" -r "/run/credentials/$own_unit/tls-key.pem"
    for path in "$neighbor" "/run/credentials/$neighbor_unit/tls-key.pem" "/run/credentials/$neighbor_unit/server.json"; do
        if "${runner[@]}" -r "$path"; then
            echo "Project UID $uid can read another project's store or credentials" >&2
            exit 1
        fi
    done
done
printf 'Verified project UIDs, access boundaries, cgroup limits and credential reload\n'

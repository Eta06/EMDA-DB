"""Runs the SQL workloads of examples/sql_bench.rs and sql_join_bench.rs on
PostgreSQL through pgbench, for comparison with EMDA and SQLite.

usage: python3 -m scripts.benchmarks.sql_postgres <pg_bin_dir> <work_dir> <out.jsonl>

A private cluster is created in <work_dir> and reached only through a Unix
socket there. Durability matches the embedded engines: fsync=on,
synchronous_commit=on, full_page_writes=on and, on macOS,
wal_sync_method=fsync_writethrough so commits flush the drive cache as
F_FULLFSYNC does. Every transaction runs SERIALIZABLE, as EMDA's do. Each
workload is one pgbench script with prepared statements and one client;
the p50 is taken from pgbench's per-transaction latency log. Tables,
indexes and rows match the Rust harnesses (10,000 rows; 2,000 users).
"""
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile

ROWS = 10_000
USERS = 2_000
OPS = 100
SCAN_OPS = 20

SETUP = f"""
CREATE TABLE items (id integer PRIMARY KEY, name text NOT NULL, score double precision, active boolean);
INSERT INTO items SELECT g, 'name-' || lpad(g::text, 8, '0'), g / 4.0, g % 3 = 0 FROM generate_series(0, {ROWS - 1}) g;
CREATE TABLE items1 (id integer PRIMARY KEY, code integer NOT NULL, tag text NOT NULL, active boolean NOT NULL, score double precision);
CREATE INDEX idx1_code ON items1 (code);
INSERT INTO items1 SELECT g, g * 3 + 7, 'tag-' || lpad((g % 200)::text, 4, '0'), g % 2 = 0, g / 4.0 FROM generate_series(0, {ROWS - 1}) g;
CREATE TABLE items3 (id integer PRIMARY KEY, code integer NOT NULL, tag text NOT NULL, active boolean NOT NULL, score double precision);
CREATE INDEX idx3_code ON items3 (code);
CREATE INDEX idx3_tag ON items3 (tag);
CREATE INDEX idx3_active ON items3 (active);
INSERT INTO items3 SELECT * FROM items1;
CREATE TABLE agg_items (id integer PRIMARY KEY, grp integer NOT NULL, val integer NOT NULL, score double precision, active boolean NOT NULL);
INSERT INTO agg_items SELECT g, g % 100, g * 3 + 1, g / 4.0, g % 3 = 0 FROM generate_series(0, {ROWS - 1}) g;
CREATE TABLE tx_items (id integer PRIMARY KEY, name text NOT NULL, score double precision, active boolean);
INSERT INTO tx_items SELECT * FROM items;
CREATE TABLE teams (id integer PRIMARY KEY, title text NOT NULL);
CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL, team integer);
CREATE TABLE orders (id integer PRIMARY KEY, user_id integer, total double precision);
CREATE INDEX orders_user ON orders (user_id);
CREATE INDEX users_team ON users (team);
INSERT INTO teams SELECT g, 'team-' || g FROM generate_series(0, 49) g;
INSERT INTO users SELECT g, 'user-' || lpad(((g * 7919) % {USERS})::text, 8, '0'), g % 50 FROM generate_series(0, {USERS - 1}) g;
INSERT INTO orders SELECT g, g / 2, (g % 97) / 4.0 FROM generate_series(0, {USERS * 2 - 1}) g;
CREATE SEQUENCE new_ids START 1000000;
"""

# name -> (pgbench script, transactions). Each script is one measured
# transaction; statements without BEGIN autocommit.
WORKLOADS = {
    # Protocol and socket round trip with no table access: the share of every
    # other PostgreSQL latency that the in-process engines do not pay.
    "roundtrip": ("SELECT 1;\n", OPS),
    "pk_select": ("\\set id random(0, 9999)\nSELECT id, name, score, active FROM items WHERE id = :id;\n", OPS),
    "insert": ("INSERT INTO items VALUES (nextval('new_ids'), 'new', 1.5, true);\n", OPS),
    "pk_update": ("\\set id random(0, 9999)\nUPDATE items SET score = -1.0 WHERE id = :id;\n", OPS),
    # Deletes rows inserted above, one per transaction, found by key range.
    "pk_delete": ("DELETE FROM items WHERE id = (SELECT min(id) FROM items WHERE id >= 1000000);\n", OPS),
    "full_select": ("SELECT id FROM items WHERE score >= 0.0 AND active = TRUE;\n", SCAN_OPS),
    "tx_10_updates": ("BEGIN;\n" + "".join(
        f"\\set id{i} random(0, 9999)\nUPDATE tx_items SET score = {i}.5 WHERE id = :id{i};\n" for i in range(10)
    ) + "COMMIT;\n", OPS),
    "idx_eq": ("\\set id random(0, 9999)\nSELECT id, code, tag, active, score FROM items1 WHERE code = :id * 3 + 7;\n", OPS),
    "idx_range_10": ("\\set id random(0, 9989)\nSELECT id, code, tag, active, score FROM items1 WHERE code >= :id * 3 + 7 AND code < :id * 3 + 37;\n", OPS),
    "insert_1idx": ("\\set id random(20000, 600000000)\nINSERT INTO items1 VALUES (:id, :id * 3 + 7, 'tag-0001', true, 1.5);\n", OPS),
    "insert_3idx": ("\\set id random(20000, 600000000)\nINSERT INTO items3 VALUES (:id, :id * 3 + 7, 'tag-0001', true, 1.5);\n", OPS),
    "count_star": ("SELECT COUNT(*) FROM agg_items;\n", SCAN_OPS),
    "sum_where": ("SELECT SUM(val) FROM agg_items WHERE active = TRUE AND score >= 0.0;\n", SCAN_OPS),
    "group_by_100": ("SELECT grp, COUNT(*), SUM(val), AVG(score) FROM agg_items GROUP BY grp;\n", SCAN_OPS),
    "point": ("\\set id random(0, 1999)\nSELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team WHERE u.id = :id;\n", OPS),
    "fanout": ("\\set id random(0, 1999)\nSELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = :id;\n", OPS),
    "team_scan": ("\\set t random(0, 49)\nSELECT u.name, t.title FROM users u JOIN teams t ON t.id = u.team WHERE u.team = :t ORDER BY u.name;\n", SCAN_OPS),
    "three_way": ("SELECT o.id, u.name, t.title FROM orders o JOIN users u ON u.id = o.user_id JOIN teams t ON t.id = u.team WHERE o.total > 20.0 ORDER BY o.id LIMIT 20;\n", SCAN_OPS),
}

CONF = """
listen_addresses = ''
unix_socket_directories = '{sock}'
port = 55433
fsync = on
synchronous_commit = on
full_page_writes = on
wal_sync_method = {sync}
default_transaction_isolation = 'serializable'
max_connections = 10
"""


def main():
    pg, work, out = sys.argv[1:4]
    work = pathlib.Path(work).resolve()
    data = work / "data"
    work.mkdir(parents=True, exist_ok=True)
    # Unix socket paths are limited to about 100 bytes, so the socket lives
    # in a short private directory (mode 0700) rather than under <work_dir>.
    sock = pathlib.Path(tempfile.mkdtemp(prefix="emda-pg-"))
    run = lambda *cmd, **kw: subprocess.run(cmd, check=True, **kw)
    run(f"{pg}/initdb", "-D", str(data), "-A", "trust", "-U", "bench", "--no-sync",
        stdout=subprocess.DEVNULL)
    sync = "fsync_writethrough" if sys.platform == "darwin" else "fdatasync"
    with open(data / "postgresql.conf", "a") as conf:
        conf.write(CONF.format(sock=sock, sync=sync))
    run(f"{pg}/pg_ctl", "-D", str(data), "-l", str(work / "server.log"), "-w", "start",
        stdout=subprocess.DEVNULL)
    conn = ["-h", str(sock), "-p", "55433", "-U", "bench"]
    try:
        run(f"{pg}/createdb", *conn, "bench")
        run(f"{pg}/psql", *conn, "-q", "-v", "ON_ERROR_STOP=1", "-d", "bench", "-c", SETUP)
        run(f"{pg}/psql", *conn, "-q", "-d", "bench", "-c", "VACUUM ANALYZE")
        settings = subprocess.run(
            [f"{pg}/psql", *conn, "-d", "bench", "-At", "-c",
             "SELECT version(); SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; "
             "SHOW wal_sync_method; SHOW default_transaction_isolation;"],
            check=True, capture_output=True, text=True).stdout
        pgbench_version = subprocess.run([f"{pg}/pgbench", "--version"], check=True,
                                         capture_output=True, text=True).stdout.strip()
        result = {"backend": "postgresql", "settings": settings.strip().splitlines(),
                  "pgbench": pgbench_version}
        for name, (script, count) in WORKLOADS.items():
            path = work / f"{name}.sql"
            path.write_text(script)
            prefix = work / f"log-{name}"
            run(f"{pg}/pgbench", *conn, "-n", "-M", "prepared", "-c", "1", "-j", "1",
                "-t", str(count), "-f", str(path), "-l", "--log-prefix", str(prefix), "bench",
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logs = list(work.glob(f"log-{name}.*"))
            lat = [int(line.split()[2]) for log in logs for line in open(log)]
            for log in logs:
                log.unlink()
            if len(lat) != count:
                raise SystemExit(f"{name}: expected {count} transactions, logged {len(lat)}")
            lat.sort()
            result[name] = {"n": len(lat), "p50_us": statistics.median(lat),
                            "p99_us": lat[min(len(lat) - 1, round(0.99 * (len(lat) - 1)))]}
        checks = subprocess.run(
            [f"{pg}/psql", *conn, "-d", "bench", "-At", "-c",
             "SELECT count(*) FROM items WHERE score >= 0.0 AND active = TRUE; "
             "SELECT count(*) FROM items WHERE id >= 1000000;"],
            check=True, capture_output=True, text=True).stdout.split()
        result["full_select_matched"], result["leftover_inserted"] = int(checks[0]), int(checks[1])
        with open(out, "a") as f:
            f.write(json.dumps(result) + "\n")
    finally:
        run(f"{pg}/pg_ctl", "-D", str(data), "-m", "fast", "-w", "stop", stdout=subprocess.DEVNULL)
        shutil.rmtree(sock, ignore_errors=True)


if __name__ == "__main__":
    main()

"""Durable embedded backends for the loopback comparison adapter only."""
import importlib.metadata
import sqlite3
import re
import time
import zlib


def pack(value):
    return value + zlib.crc32(value).to_bytes(4, "big")


def unpack(payload):
    if payload is None:
        return None
    if len(payload) < 4 or zlib.crc32(payload[:-4]) != int.from_bytes(payload[-4:], "big"):
        raise RuntimeError("embedded value checksum mismatch")
    return payload[:-4]


class SqlWorker:
    def __init__(self, connection, backend, counters):
        self.conn, self.backend, self.counters = connection, backend, counters

    def execute(self, sql, parameters):
        deadline = time.monotonic() + 30
        while True:
            try:
                return self.conn.execute(sql, parameters)
            except Exception as error:
                text = str(error).lower()
                retryable = ((self.backend == "duckdb" and ("conflict on update" in text or "conflict on tuple deletion" in text or "conflict on insert" in text or "duplicate key" in text))
                             or (self.backend == "turso" and ("database is locked" in text or "database is busy" in text)))
                if not retryable or time.monotonic() >= deadline:
                    raise
                self.counters["conflict_retries"] += 1
                time.sleep(0.001)

    def get(self, key):
        row = self.execute("SELECT value, checksum FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        value = bytes(row[0])
        if zlib.crc32(value) != row[1]:
            raise RuntimeError("SQL value checksum mismatch")
        return value

    def put(self, key, value):
        self.execute("INSERT INTO kv(key,value,checksum) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, checksum=excluded.checksum", (key, value, zlib.crc32(value)))

    def delete(self, key):
        self.execute("DELETE FROM kv WHERE key = ?", (key,))

    def close(self):
        self.conn.close()


class LmdbWorker:
    def __init__(self, env):
        self.env = env

    def get(self, key):
        with self.env.begin() as txn:
            return unpack(txn.get(key))

    def put(self, key, value):
        with self.env.begin(write=True) as txn:
            txn.put(key, pack(value))

    def delete(self, key):
        with self.env.begin(write=True) as txn:
            txn.delete(key)

    def close(self):
        pass


class RocksWorker:
    def __init__(self, db, write_options):
        self.db, self.write_options = db, write_options

    def get(self, key):
        return unpack(self.db.get(key))

    def put(self, key, value):
        self.db.put(key, pack(value), self.write_options)

    def delete(self, key):
        self.db.delete(key, self.write_options)

    def close(self):
        pass


class Backend:
    def __init__(self, backend, directory):
        self.backend, self.directory = backend, directory
        self.counters = {"conflict_retries": 0}
        self.metadata = {"adapter": "Python bindings over loopback", "backend": backend}
        if backend in ("sqlite_python", "turso", "duckdb"):
            if backend == "sqlite_python":
                self.metadata.update(version=sqlite3.sqlite_version, driver="Python sqlite3")
            elif backend == "turso":
                self.metadata.update(version=importlib.metadata.version("pyturso"), driver="pyturso (Turso Rust engine, not libSQL)")
            else:
                import duckdb
                self.root = duckdb.connect(str(directory / "duckdb.db"), config={"threads": "3"})
                self.metadata.update(version=duckdb.__version__, durability="default WAL commits; no unsafe settings", threads=3)
            connection = self.connect()
            connection.execute("CREATE TABLE IF NOT EXISTS kv(key BLOB PRIMARY KEY, value BLOB NOT NULL, checksum BIGINT NOT NULL)")
            if backend != "duckdb":
                journal = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
                if journal != "wal" or synchronous != 2:
                    raise RuntimeError("WAL/FULL required")
                self.metadata.update(journal_mode=journal, synchronous=synchronous)
            connection.close()
        elif backend == "lmdb":
            import lmdb
            self.env = lmdb.open(str(directory / "lmdb"), map_size=1024**3, sync=True, metasync=True,
                                 writemap=False, map_async=False, lock=True, max_readers=128)
            self.metadata.update(version=lmdb.version(), binding=lmdb.__version__, flags=self.env.flags(), map_size=1024**3)
        elif backend == "rocksdb":
            import rocksdict
            options = rocksdict.Options(raw_mode=True)
            options.create_if_missing(True)
            self.db = rocksdict.Rdict(str(directory / "rocksdb"), options)
            self.write_options = rocksdict.WriteOptions()
            self.write_options.sync = True
            self.write_options.disable_wal = False
            match = re.search(r"RocksDB version: ([0-9.]+)", (directory / "rocksdb" / "LOG").read_text())
            self.metadata.update(version=match.group(1) if match else "not reported", binding=importlib.metadata.version("rocksdict"), sync=True, disable_wal=False, raw_mode=True)
        else:
            raise ValueError("unknown embedded backend")

    def connect(self):
        if self.backend == "duckdb":
            return self.root.cursor()
        if self.backend == "turso":
            import turso
            conn = turso.connect(str(self.directory / "turso.db"), isolation_level=None)
        else:
            conn = sqlite3.connect(str(self.directory / "sqlite.db"), isolation_level=None, timeout=30)
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def worker(self):
        if self.backend == "lmdb":
            return LmdbWorker(self.env)
        if self.backend == "rocksdb":
            return RocksWorker(self.db.get_column_family("default"), self.write_options)
        return SqlWorker(self.connect(), self.backend, self.counters)

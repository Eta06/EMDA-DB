"""Optional benchmark drivers; connections target disposable local services."""
import os
import socket
import struct
import zlib

def exact(sock, size):
    parts = bytearray()
    while len(parts) < size:
        data = sock.recv(size - len(parts))
        if not data:
            raise RuntimeError("benchmark server closed the connection")
        parts.extend(data)
    return bytes(parts)


class AdapterClient:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def request(self, operation, key, value=b""):
        self.sock.sendall(struct.pack(">cII", operation, len(key), len(value)) + key + value)
        status, size = struct.unpack(">BI", exact(self.sock, 5))
        data = exact(self.sock, size)
        if status == 2 and operation == b"R":
            return None
        if status:
            raise RuntimeError(data.decode("utf-8", "replace"))
        return data

    def get(self, key):
        return self.request(b"R", key)

    def put(self, key, value):
        self.request(b"W", key, value)

    def delete(self, key):
        self.request(b"D", key)

    def close(self):
        self.sock.close()


class PostgresClient:
    def __init__(self, table):
        import psycopg
        self.table = table  # Generated locally from a fixed prefix and hex digits.
        self.conn = psycopg.connect(
            host="127.0.0.1", port=int(os.environ["EMDA_BENCH_PG_PORT"]),
            dbname="benchmark", user="postgres", password=os.environ["EMDA_BENCH_PG_PASSWORD"],
            autocommit=True, prepare_threshold=0, connect_timeout=10,
        )

    def get(self, key):
        row = self.conn.execute(f"SELECT value, checksum FROM {self.table} WHERE key = %s", (key,)).fetchone()
        if row is None:
            return None
        value, checksum = bytes(row[0]), row[1]
        if zlib.crc32(value) != checksum:
            raise RuntimeError("PostgreSQL value checksum mismatch")
        return value

    def put(self, key, value):
        self.conn.execute(
            f"INSERT INTO {self.table} (key, value, checksum) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, checksum=EXCLUDED.checksum",
            (key, value, zlib.crc32(value)),
        )

    def delete(self, key):
        self.conn.execute(f"DELETE FROM {self.table} WHERE key = %s", (key,))

    def close(self):
        self.conn.close()


class RedisClient:
    def __init__(self, prefix):
        import redis
        self.prefix = prefix
        self.conn = redis.Redis(host="127.0.0.1", port=int(os.environ["EMDA_BENCH_REDIS_PORT"]), socket_timeout=30)

    def get(self, key):
        payload = self.conn.get(self.prefix + key)
        if payload is None:
            return None
        if len(payload) < 4:
            raise RuntimeError("missing or incomplete Redis value")
        value, checksum = payload[:-4], int.from_bytes(payload[-4:], "big")
        if zlib.crc32(value) != checksum:
            raise RuntimeError("Redis value checksum mismatch")
        return value

    def put(self, key, value):
        self.conn.set(self.prefix + key, value + zlib.crc32(value).to_bytes(4, "big"))

    def delete(self, key):
        self.conn.delete(self.prefix + key)

    def close(self):
        self.conn.close()


class MongoClient:
    def __init__(self, collection):
        from pymongo import MongoClient as Driver
        from pymongo.write_concern import WriteConcern
        self.conn = Driver(host="127.0.0.1", port=int(os.environ["EMDA_BENCH_MONGO_PORT"]),
                           serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
        self.collection = self.conn.benchmark.get_collection(collection, write_concern=WriteConcern(w=1, j=True, wtimeout=30000))

    def get(self, key):
        row = self.collection.find_one({"_id": key})
        if row is None:
            return None
        value = bytes(row["value"])
        if zlib.crc32(value) != row["checksum"]:
            raise RuntimeError("MongoDB checksum mismatch")
        return value

    def put(self, key, value):
        self.collection.replace_one({"_id": key}, {"_id": key, "value": value, "checksum": zlib.crc32(value)}, upsert=True)

    def delete(self, key):
        self.collection.delete_one({"_id": key})

    def close(self):
        self.conn.close()


class FoundationClient:
    def __init__(self, prefix):
        import fdb
        fdb.api_version(730)
        self.conn = fdb.open(os.environ["EMDA_BENCH_FDB_CLUSTER"])
        self.conn.options.set_transaction_timeout(30000)
        self.prefix = prefix

    def get(self, key):
        value = self.conn[self.prefix + key]
        if value is None:
            return None
        if len(value) < 4 or zlib.crc32(value[:-4]) != int.from_bytes(value[-4:], "big"):
            raise RuntimeError("FoundationDB checksum mismatch")
        return value[:-4]

    def put(self, key, value):
        self.conn[self.prefix + key] = value + zlib.crc32(value).to_bytes(4, "big")

    def delete(self, key):
        del self.conn[self.prefix + key]

    def close(self):
        pass  # The FDB binding owns the shared network thread and database handle.

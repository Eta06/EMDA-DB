"""Disposable local servers; only containers created by this object are managed."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time

IMAGES = {"postgres": "postgres:18-bookworm", "redis": "redis:8.2-bookworm",
          "mongo": "mongo:8.0", "foundationdb": "foundationdb/foundationdb:7.3.79"}


def docker(*args, environment=None, timeout=120):
    env = os.environ.copy()
    env.update(environment or {})
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        env.pop(name, None)
    return subprocess.check_output(["docker", "--host=unix:///var/run/docker.sock", *args],
                                   env=env, text=True, stderr=subprocess.PIPE, timeout=timeout).strip()


class LocalService:
    def __init__(self, backend, parent, cpus="1-3"):
        self.backend = backend
        self.temp = tempfile.TemporaryDirectory(prefix=".comparison-" + backend + "-", dir=parent)
        self.path = Path(self.temp.name)
        self.path.chmod(0o755)
        self.name = "emda-compare-" + backend + "-" + secrets.token_hex(5)
        self.env = {}
        self.initialized = False
        image = IMAGES[backend]
        self.image = json.loads(docker("image", "inspect", image))[0]
        args = ["create", "--name", self.name, "--cpuset-cpus", cpus, "--memory", "2g",
                "--label", "emda.benchmark=disposable", "--mount", f"type=bind,src={self.path},dst=/data"]
        try:
            if backend == "postgres":
                self.env["EMDA_BENCH_PG_PASSWORD"] = secrets.token_hex(16)
                args += ["-p", "127.0.0.1::5432", "-e", "PGDATA=/data/pgdata", "-e", "POSTGRES_DB=benchmark",
                         "-e", "POSTGRES_PASSWORD", image, "postgres", "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "full_page_writes=on"]
                docker(*args, environment={"POSTGRES_PASSWORD": self.env["EMDA_BENCH_PG_PASSWORD"]})
            elif backend == "redis":
                args += ["-p", "127.0.0.1::6379", image, "redis-server", "--dir", "/data", "--appendonly", "yes",
                         "--appendfsync", "always", "--save", "", "--no-appendfsync-on-rewrite", "no"]
                docker(*args)
            elif backend == "mongo":
                args += ["-p", "127.0.0.1::27017", image, "mongod", "--dbpath", "/data/db", "--bind_ip_all",
                         "--journalCommitInterval", "1", "--wiredTigerCacheSizeGB", "0.5"]
                docker(*args)
            else:
                with socket.socket() as probe:
                    probe.bind(("127.0.0.1", 0))
                    self.port = probe.getsockname()[1]
                cluster = self.path / "fdb.cluster"
                cluster.write_text(f"benchmark:{secrets.token_hex(8)}@127.0.0.1:{self.port}\n")
                self.env["EMDA_BENCH_FDB_CLUSTER"] = str(cluster)
                # Run as our uid so the cluster/config files remain removable.
                args += ["--user", f"{os.getuid()}:{os.getgid()}", "-p", f"127.0.0.1:{self.port}:{self.port}",
                         "--entrypoint", "/usr/bin/fdbserver", image, "--cluster-file", "/data/fdb.cluster",
                         "--public-address", f"127.0.0.1:{self.port}", "--listen-address", f"0.0.0.0:{self.port}",
                         "--datadir", "/data/db", "--logdir", "/data/logs", "--locality-machineid", self.name,
                         "--memory", "1GiB", "--storage-memory", "256MiB", "--cache-memory", "256MiB"]
                docker(*args)
        except BaseException:
            self.close()
            raise

    def start(self):
        docker("start", self.name)
        if self.backend != "foundationdb":
            port = {"postgres": 5432, "redis": 6379, "mongo": 27017}[self.backend]
            bound = docker("port", self.name, f"{port}/tcp").splitlines()[0]
            self.port = int(bound.rsplit(":", 1)[1])
            self.env[{"postgres": "EMDA_BENCH_PG_PORT", "redis": "EMDA_BENCH_REDIS_PORT", "mongo": "EMDA_BENCH_MONGO_PORT"}[self.backend]] = str(self.port)
        deadline = time.monotonic() + 60
        last_error = None
        while time.monotonic() < deadline:
            try:
                self.ready()
                return
            except Exception as error:
                last_error = error
                time.sleep(0.25)
        raise RuntimeError(f"{self.backend} did not become ready: {type(last_error).__name__}: {last_error}")

    def ready(self):
        if self.backend == "postgres":
            import psycopg
            with psycopg.connect(host="127.0.0.1", port=self.port, dbname="benchmark", user="postgres",
                                password=self.env["EMDA_BENCH_PG_PASSWORD"], connect_timeout=2) as conn:
                conn.execute("SELECT 1")
        elif self.backend == "redis":
            import redis
            conn = redis.Redis(host="127.0.0.1", port=self.port, socket_connect_timeout=2)
            try:
                conn.ping()
            finally:
                conn.close()
        elif self.backend == "mongo":
            from pymongo import MongoClient
            with MongoClient(host="127.0.0.1", port=self.port, serverSelectionTimeoutMS=1000) as conn:
                conn.admin.command("ping")
        else:
            if not self.initialized:
                docker("exec", self.name, "/usr/bin/fdbcli", "-C", "/data/fdb.cluster", "--timeout", "5", "--exec", "configure new single ssd", timeout=10)
                self.initialized = True
            status = json.loads(docker("exec", self.name, "/usr/bin/fdbcli", "-C", "/data/fdb.cluster", "--timeout", "5", "--exec", "status json", timeout=10))
            if not status.get("client", {}).get("database_status", {}).get("available"):
                raise RuntimeError("FoundationDB is not available yet")

    def stop(self):
        docker("stop", "--time", "15", self.name, timeout=30)

    def kill(self):
        docker("kill", "--signal", "KILL", self.name)

    def metadata(self):
        result = {"image": IMAGES[self.backend], "image_id": self.image["Id"], "digests": self.image.get("RepoDigests"),
                  "container_cpu_set": json.loads(docker("inspect", self.name))[0]["HostConfig"]["CpusetCpus"],
                  "memory_limit_bytes": 2 * 1024**3, "port_binding": "127.0.0.1 only"}
        if self.backend == "foundationdb":
            result["status"] = json.loads(docker("exec", self.name, "/usr/bin/fdbcli", "-C", "/data/fdb.cluster", "--exec", "status json"))
        return result

    def close(self):
        try:
            docker("rm", "-f", self.name)
        except subprocess.CalledProcessError:
            pass
        # Database images use their own uid. Remove only our dedicated data
        # directory using a short-lived helper, never a shared host directory.
        try:
            self.temp.cleanup()
        except PermissionError:
            docker("run", "--rm", "--user", "0", "--entrypoint", "/bin/sh", "--mount",
                   f"type=bind,src={self.path},dst=/cleanup", "redis:8.2-bookworm", "-c",
                   "find /cleanup -mindepth 1 -delete")
            self.temp.cleanup()

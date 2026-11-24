"""Loopback server adapters for the concurrent document contention fixture."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from pathlib import Path
from threading import Barrier
import time

from .telemetry import key_values, read


def expected_row(number, version):
    identifier = f"doc-{number:06}"
    return identifier, {
        "email": f"{identifier}-{version:06}@example.test",
        "version": version,
        "active": True,
    }


def verify_row(rows, expected):
    if (rows != [expected] or type(rows[0][1]["version"]) is not int
            or rows[0][1]["active"] is not True):
        raise ValueError(f"document {expected[0]} does not match its acknowledged replacement")


def verify_reopened(rows, lookup, documents, clients, cycles):
    if len(rows) != documents:
        raise ValueError("reopened primary row count differs from the fixture")
    for number in range(documents):
        expected = expected_row(number, cycles if number < clients else 0)
        verify_row([rows[number]], expected)
        verify_row(lookup(expected[1]["email"]), expected)


def server_scope(service):
    pid = service.container_state().get("Pid")
    membership = read(f"/proc/{pid}/cgroup") if type(pid) is int and pid > 0 else None
    if membership is None:
        return None
    for line in membership.splitlines():
        if line.startswith("0::/") and line != "0::/":
            root = Path("/sys/fs/cgroup")
            scope = (root / line[4:]).resolve()
            if root in scope.parents and scope.is_dir():
                return scope
    return None


def resources(scope):
    client_status = read("/proc/self/status") or ""
    rss = next((int(line.split()[1]) for line in client_status.splitlines()
                if line.startswith("VmRSS:")), None)
    return {
        "client_rss_kib": rss,
        "client_cpu_ns": time.process_time_ns(),
        "server_cgroup_cpu": key_values(scope / "cpu.stat") if scope else None,
        "server_cgroup_memory_bytes": read(scope / "memory.current") if scope else None,
        "server_cgroup_io": read(scope / "io.stat") if scope else None,
    }


def run_clients(operations, cycles, result, scope):
    barrier = Barrier(len(operations) + 1, timeout=30)
    samples = [
        {"update_ns": [], "query_ns": [], "update_retries": 0, "query_retries": 0}
        for _ in operations
    ]
    result["clients_samples"] = samples

    def client(number, replace, lookup):
        barrier.wait()
        for version in range(1, cycles + 1):
            identifier, body = expected_row(number, version)
            start = time.perf_counter_ns()
            if replace(identifier, body) != 1:
                raise ValueError(f"replacement did not match document {identifier}")
            samples[number]["update_ns"].append(time.perf_counter_ns() - start)
            start = time.perf_counter_ns()
            verify_row(lookup(body["email"]), (identifier, body))
            samples[number]["query_ns"].append(time.perf_counter_ns() - start)

    with ThreadPoolExecutor(max_workers=len(operations)) as workers:
        tasks = [workers.submit(client, number, *pair) for number, pair in enumerate(operations)]
        result["resources_before"] = resources(scope)
        start = time.perf_counter_ns()
        barrier.wait()
        try:
            for task in tasks:
                task.result()
            elapsed = time.perf_counter_ns() - start
            result.update(elapsed_ns=elapsed, ops_per_sec=len(tasks) * cycles * 2e9 / elapsed)
        finally:
            result["resources_after"] = resources(scope)


@contextmanager
def postgres_session(service):
    import psycopg

    with psycopg.connect(host="127.0.0.1", port=service.port, dbname="benchmark",
                         user="postgres", password=service.env["EMDA_BENCH_PG_PASSWORD"],
                         connect_timeout=5, autocommit=True,
                         options="-c statement_timeout=30000 -c lock_timeout=30000") as conn:
        conn.execute("SET enable_seqscan=off")
        yield conn


def postgres(service, documents, clients, cycles, result):
    import psycopg
    from psycopg.types.json import Jsonb

    statement = "SELECT id, body FROM docs WHERE body->>'email'=%s LIMIT 2"

    def operations(conn):
        def replace(identifier, body):
            return conn.execute("UPDATE docs SET body=%s WHERE id=%s",
                                (Jsonb(body), identifier)).rowcount
        def lookup(email):
            return conn.execute(statement, (email,)).fetchall()
        return replace, lookup

    with postgres_session(service) as conn:
        settings = {key: conn.execute("SHOW " + key).fetchone()[0]
                    for key in ("fsync", "synchronous_commit", "full_page_writes")}
        if any(value != "on" for value in settings.values()):
            raise ValueError("PostgreSQL durability settings did not match the workload")
        conn.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body JSONB NOT NULL)")
        conn.execute("CREATE UNIQUE INDEX docs_email ON docs ((body->>'email'))")
        with conn.transaction(), conn.cursor() as cursor:
            cursor.executemany("INSERT INTO docs VALUES (%s, %s)",
                               [(identifier, Jsonb(body)) for identifier, body in
                                (expected_row(number, 0) for number in range(documents))])
        plan = [line for (line,) in conn.execute("EXPLAIN " + statement, ("probe@example.test",))]
        if not any("Index Scan using docs_email" in line for line in plan):
            raise ValueError("PostgreSQL did not select the email index")
        result["configuration"] = dict(durability=settings, query_plan=plan,
                                       version=conn.execute("SHOW server_version").fetchone()[0],
                                       driver_version=psycopg.__version__, transport="psycopg loopback")
    with ExitStack() as stack:
        connections = [stack.enter_context(postgres_session(service)) for _ in range(clients)]
        run_clients([operations(conn) for conn in connections], cycles, result, server_scope(service))
    service.stop()
    service.start()
    with postgres_session(service) as conn:
        rows = conn.execute('SELECT id, body FROM docs ORDER BY id COLLATE "C"').fetchall()
        verify_reopened(rows, operations(conn)[1], documents, clients, cycles)


def mongo(service, documents, clients, cycles, result):
    import pymongo
    from pymongo.write_concern import WriteConcern

    def connect():
        return pymongo.MongoClient(host="127.0.0.1", port=service.port, serverSelectionTimeoutMS=5000,
                                   timeoutMS=30000, retryWrites=False, retryReads=False)

    def collection(conn):
        return conn.benchmark.get_collection("docs", write_concern=WriteConcern(w=1, j=True))

    def decode(row):
        identifier = row.pop("_id")
        return identifier, row

    def operations(conn):
        docs = collection(conn)
        def replace(identifier, body):
            return docs.replace_one({"_id": identifier}, {"_id": identifier, **body}).matched_count
        def lookup(email):
            return [decode(row) for row in docs.find({"email": email}).hint("docs_email").limit(2)]
        return replace, lookup

    with connect() as conn:
        docs = collection(conn)
        docs.create_index("email", unique=True, name="docs_email")
        docs.insert_many([{"_id": identifier, **body} for identifier, body in
                          (expected_row(number, 0) for number in range(documents))])
        plan = docs.find({"email": "probe@example.test"}).hint("docs_email").limit(2).explain()
        from compound_range_external_compare import uses_index
        winning_plan = plan["queryPlanner"]["winningPlan"]
        if not uses_index(winning_plan, "docs_email"):
            raise ValueError("MongoDB did not select the email index")
        result["configuration"] = dict(durability={"w": 1, "j": True}, query_plan=winning_plan,
                                       version=conn.server_info()["version"], driver_version=pymongo.version,
                                       transport="PyMongo loopback")
    with ExitStack() as stack:
        connections = [stack.enter_context(connect()) for _ in range(clients)]
        for conn in connections:
            conn.admin.command("ping")
        run_clients([operations(conn) for conn in connections], cycles, result, server_scope(service))
    service.stop()
    service.start()
    with connect() as conn:
        rows = [decode(row) for row in collection(conn).find().sort("_id")]
        verify_reopened(rows, operations(conn)[1], documents, clients, cycles)

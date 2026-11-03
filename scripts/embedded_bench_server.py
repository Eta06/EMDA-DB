#!/usr/bin/env python3
"""Loopback-only benchmark adapter. No authentication or public-service API."""
import json
from pathlib import Path
import socket
import socketserver
import struct
import sys

from benchmarks.clients import exact
from benchmarks.embedded import Backend


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        worker = self.server.backend.worker()
        try:
            while True:
                try:
                    operation, key_size, value_size = struct.unpack(">cII", exact(self.request, 9))
                except RuntimeError:
                    return
                if not 1 <= key_size <= 1024 or value_size > 16 * 1024**2:
                    return
                key, value = exact(self.request, key_size), exact(self.request, value_size)
                try:
                    if operation == b"R" and value_size == 0:
                        result = worker.get(key)
                    elif operation == b"W":
                        worker.put(key, value)
                        result = b""
                    elif operation == b"D" and value_size == 0:
                        worker.delete(key)
                        result = b""
                    elif operation == b"M":
                        result = json.dumps({**self.server.backend.metadata, **self.server.backend.counters}).encode()
                    else:
                        raise ValueError("invalid benchmark operation")
                    status = 2 if result is None else 0
                    result = result or b""
                except Exception as error:
                    status, result = 1, f"{type(error).__name__}: {error}".encode()
                # Match the native adapter's response writes to avoid changing
                # transport framing only for one implementation.
                self.request.sendall(bytes([status]))
                self.request.sendall(struct.pack(">I", len(result)))
                if result:
                    self.request.sendall(result)
        finally:
            worker.close()


class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True


def main():
    backend, directory = sys.argv[1:]
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError("benchmark directory must exist")
    with Server(("127.0.0.1", 0), Handler) as server:
        server.backend = Backend(backend, directory)
        print("READY", server.server_address[1], flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()

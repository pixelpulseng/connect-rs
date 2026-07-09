#!/usr/bin/env python3
"""End-to-end tests for nonolith-connect over real sockets.

Covers the layers the in-process doctest suite cannot reach: server.cpp
(accept loop, origin checks, redirect, WS upgrade) and session.cpp
(Beast HTTP/WS sessions). Runs the real binary on an ephemeral port; no
USB device is required (device-less endpoints only).

Usage: e2e.py /path/to/nonolith-connect

Python stdlib only, including a minimal RFC 6455 WebSocket client.
"""

import base64
import hashlib
import http.client
import json
import os
import socket
import struct
import subprocess
import sys
import time

FAILURES = []


def check(name, cond, detail=""):
    status = "ok" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" -- {detail}" if not cond and detail else ""))
    if not cond:
        FAILURES.append(name)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_for_server(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def request(port, method, path, headers=None, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


class WebSocket:
    """Minimal RFC 6455 client: handshake, text frames, close."""

    def __init__(self, port, path="/ws/v0", origin=None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        headers = [
            f"GET {path} HTTP/1.1",
            f"Host: 127.0.0.1:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
        ]
        if origin:
            headers.append(f"Origin: {origin}")
        self.sock.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())

        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("closed during handshake")
            response += chunk
        status = response.split(b"\r\n", 1)[0]
        if b"101" not in status:
            raise ConnectionError(f"handshake rejected: {status!r}")
        accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        ).decode()
        if accept.encode() not in response:
            raise ConnectionError("bad Sec-WebSocket-Accept")
        self.buf = response.split(b"\r\n\r\n", 1)[1]

    def _recv_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def recv_frame(self):
        b1, b2 = self._recv_exact(2)
        opcode = b1 & 0x0F
        length = b2 & 0x7F
        if length == 126:
            (length,) = struct.unpack(">H", self._recv_exact(2))
        elif length == 127:
            (length,) = struct.unpack(">Q", self._recv_exact(8))
        payload = self._recv_exact(length)
        return opcode, payload

    def recv_json(self, timeout=5.0):
        self.sock.settimeout(timeout)
        while True:
            opcode, payload = self.recv_frame()
            if opcode == 1:  # text
                return json.loads(payload)
            if opcode == 8:  # close
                raise ConnectionError("closed by server")
            # ignore pings/binary here

    def send_text(self, message):
        payload = message.encode()
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        header = bytes([0x81])
        n = len(payload)
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 1 << 16:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.sock.sendall(header + mask + masked)

    def send_json(self, obj):
        self.send_text(json.dumps(obj))

    def close(self):
        try:
            self.sock.sendall(bytes([0x88, 0x80]) + os.urandom(4))
            self.sock.close()
        except OSError:
            pass


def main():
    if len(sys.argv) != 2:
        print("usage: e2e.py /path/to/nonolith-connect", file=sys.stderr)
        return 2

    binary = sys.argv[1]
    port = free_port()
    print(f"starting {binary} on port {port}")
    proc = subprocess.Popen(
        [binary, f"port={port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_for_server(port):
            print("server did not start", file=sys.stderr)
            return 1
        run_tests(port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    if FAILURES:
        print(f"\n{len(FAILURES)} failure(s): {', '.join(FAILURES)}")
        return 1
    print("\nall e2e tests passed")
    return 0


def run_tests(port):
    print("REST:")
    status, headers, body = request(port, "GET", "/rest/v1/")
    check("GET /rest/v1/ is 200", status == 200)
    info = json.loads(body)
    check("server identifies itself", info.get("server") == "Nonolith Connect")
    check("version present", "version" in info and "gitVersion" in info)

    status, _, body = request(port, "GET", "/rest/v1/devices")
    check("GET /rest/v1/devices is 200", status == 200)
    check("device list is an object", isinstance(json.loads(body), dict))

    status, headers, _ = request(port, "GET", "/")
    check("GET / redirects", status == 301)
    check("redirect has Location", "Location" in headers)

    status, _, _ = request(port, "GET", "/nonexistent")
    check("unknown path is 404", status == 404)

    status, _, body = request(port, "GET", "/rest/v1/devices/not.a.device~X")
    check("unknown device is 404", status == 404, f"got {status}")

    status, _, _ = request(port, "GET", "/rest")
    check("GET /rest (no version) is 404", status == 404)

    status, _, _ = request(port, "GET", "/rest/v9/devices")
    check("unsupported version is 404", status == 404)

    print("Origin checks:")
    status, _, _ = request(port, "GET", "/rest/v1/",
                           headers={"Origin": "http://evil.example.com"})
    check("foreign origin rejected with 403", status == 403)

    for origin in ("http://localhost:5173", "https://www.nonolithlabs.com", "null"):
        status, _, _ = request(port, "GET", "/rest/v1/", headers={"Origin": origin})
        check(f"origin {origin} allowed", status == 200)

    status, headers, _ = request(port, "GET", "/rest/v1/",
                                 headers={"Origin": "http://localhost:5173"})
    check("CORS header echoes origin",
          headers.get("Access-Control-Allow-Origin") == "http://localhost:5173")

    print("WebSocket:")
    ws = WebSocket(port)
    hello = ws.recv_json()
    check("serverHello first", hello.get("_action") == "serverHello")
    check("hello has version", "version" in hello and "gitVersion" in hello)

    devs = ws.recv_json()
    check("devices message second", devs.get("_action") == "devices")
    check("devices is an object", isinstance(devs.get("devices"), dict))

    # errors carry _action: error
    ws.send_text("this is not json")
    err = ws.recv_json()
    check("invalid JSON yields error action", err.get("_action") == "error")

    ws.send_json({"foo": 1})
    err = ws.recv_json()
    check("missing _cmd yields error action", err.get("_action") == "error")

    # selecting a nonexistent device: server logs and stays silent, and the
    # connection remains usable
    ws.send_json({"_cmd": "selectDevice", "id": "nope~X"})
    ws.send_text("still not json")
    err = ws.recv_json()
    check("connection still alive after bad selectDevice",
          err.get("_action") == "error")
    ws.close()

    # a second concurrent WS connection works
    ws1 = WebSocket(port)
    ws2 = WebSocket(port)
    check("two concurrent WS clients get hellos",
          ws1.recv_json().get("_action") == "serverHello"
          and ws2.recv_json().get("_action") == "serverHello")
    ws1.close()
    ws2.close()

    # WS upgrade with a foreign origin: Beast accepts the upgrade before the
    # origin check layer, so just verify it doesn't take the server down
    try:
        wsf = WebSocket(port, origin="http://evil.example.com")
        wsf.close()
    except ConnectionError:
        pass
    status, _, _ = request(port, "GET", "/rest/v1/")
    check("server alive after foreign-origin WS attempt", status == 200)

    # upgrade on a non-/ws path must not be treated as WebSocket
    try:
        WebSocket(port, path="/rest/v1/")
        check("upgrade on non-ws path rejected", False)
    except (ConnectionError, OSError):
        check("upgrade on non-ws path rejected", True)


if __name__ == "__main__":
    sys.exit(main())

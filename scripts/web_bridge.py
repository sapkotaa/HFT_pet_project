#!/usr/bin/env python3
"""HTTP/SSE bridge between the gateway's raw binary TCP protocol and a
browser. Browsers can't open raw TCP sockets, so this process holds one
persistent, logged-on session to the gateway and re-exposes it as plain
HTTP: POST /api/order and /api/cancel to send, GET /events (Server-Sent
Events) for a live stream of order state as exec reports arrive.

Also serves GET /api/history from the gateway's SQLite database (written
by DbConsumer, see include/db_consumer.hpp) — order/execution history
that survives a bridge restart, unlike the in-memory blotter above.
Opened read-only, per request; safe to read concurrently with the
gateway's writer because the DB runs in WAL journal mode.

Stdlib only, deliberately — this is a demo/ops bridge, not part of the
matching engine's hot path, so there's no reason to pull in a dependency
for it.

Usage:
    python3 scripts/web_bridge.py [--gateway-host 127.0.0.1] [--gateway-port 9000]
                                   [--http-port 8080] [--db-path data/hft_lob.db]
Then open http://127.0.0.1:8080/ in a browser.
"""
import argparse
import json
import os
import queue
import socket
import sqlite3
import struct
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HEADER_FMT = "<HBI"           # body_length, msg_type, seq_num
HEADER_LEN = struct.calcsize(HEADER_FMT)
LOGON_FMT = "<QII"             # client_id, heartbeat_interval_ms, starting_seq_num
NEW_ORDER_FMT = "<Q8sBII"      # client_order_id, symbol[8], side, price, quantity
CANCEL_ORDER_FMT = "<Q"        # client_order_id
EXEC_REPORT_FMT = "<QQBIII"    # client_order_id, exchange_order_id, status, fill_price, fill_qty, leaves_qty

MSG_TYPE = {
    "Logon": 1, "Logout": 2, "Heartbeat": 3, "TestRequest": 4, "ResendRequest": 5,
    "NewOrder": 10, "CancelOrder": 11, "ReplaceOrder": 12,
    "ExecutionReport": 20, "Reject": 21,
}
EXEC_STATUS = {1: "Accepted", 2: "Rejected", 3: "Filled", 4: "PartialFill", 5: "Canceled"}
SIDE = {"buy": 0, "sell": 1}
SIDE_NAME = {0: "buy", 1: "sell"}

UI_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ui")


class GatewayLink:
    """Owns the single TCP session to the gateway. One reader thread
    parses inbound frames and fans them out; senders (HTTP handler
    threads) write under a lock so frames from concurrent requests never
    interleave on the wire."""

    def __init__(self, host, port, client_id=424242):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.sock = None
        self.send_lock = threading.Lock()
        self.connected = threading.Event()
        self.seq = 0
        self.order_counter_lock = threading.Lock()
        self.order_counter = 0

        self.orders_lock = threading.Lock()
        self.orders = {}          # client_order_id -> order dict, insertion order preserved
        self.subscribers = set()  # set[queue.Queue] — one per live SSE connection

        self.stats_lock = threading.Lock()
        self.stats = {"orders_sent": 0, "fills": 0, "rejects": 0, "cancels": 0}

        threading.Thread(target=self._run, daemon=True).start()

    # ---- public API used by HTTP handlers ----

    def next_client_order_id(self):
        with self.order_counter_lock:
            self.order_counter += 1
            return self.order_counter

    def submit_new_order(self, side, price, quantity):
        client_order_id = self.next_client_order_id()
        body = struct.pack(NEW_ORDER_FMT, client_order_id, b"UI".ljust(8, b"\0"),
                            SIDE[side], int(price), int(quantity))
        order = {
            "client_order_id": client_order_id, "exchange_order_id": 0,
            "side": side, "price": int(price), "quantity": int(quantity),
            "status": "Submitted", "fill_price": 0, "fill_quantity": 0,
            "leaves_quantity": int(quantity), "ts": time.time(),
        }
        with self.orders_lock:
            self.orders[client_order_id] = order
        self._broadcast({"type": "update", "order": dict(order)})
        with self.stats_lock:
            self.stats["orders_sent"] += 1
        self._send_frame("NewOrder", body)
        return client_order_id

    def submit_cancel(self, client_order_id):
        body = struct.pack(CANCEL_ORDER_FMT, client_order_id)
        self._send_frame("CancelOrder", body)

    def snapshot(self):
        with self.orders_lock:
            orders = list(self.orders.values())
        with self.stats_lock:
            stats = dict(self.stats)
        return orders, stats

    def subscribe(self):
        q = queue.Queue()
        with self.orders_lock:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q):
        with self.orders_lock:
            self.subscribers.discard(q)

    # ---- internals ----

    def _broadcast(self, msg):
        with self.orders_lock:
            subs = list(self.subscribers)
        for q in subs:
            q.put(msg)

    def _send_frame(self, msg_type, body):
        if not self.connected.is_set():
            return False
        self.seq += 1
        header = struct.pack(HEADER_FMT, len(body), MSG_TYPE[msg_type], self.seq)
        try:
            with self.send_lock:
                self.sock.sendall(header + body)
            return True
        except OSError:
            return False

    def _run(self):
        backoff = 0.5
        while True:
            try:
                self._connect_and_pump()
            except OSError as e:
                print(f"[bridge] gateway link error: {e}", file=sys.stderr)
            self.connected.clear()
            self._broadcast({"type": "status", "connected": False})
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)

    def _connect_and_pump(self):
        print(f"[bridge] connecting to gateway {self.host}:{self.port} ...")
        self.sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sock.settimeout(1.0)
        self.seq = 0
        logon_body = struct.pack(LOGON_FMT, self.client_id, 5000, 0)
        self._raw_send("Logon", logon_body)

        buf = b""
        while True:
            try:
                chunk = self.sock.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                raise OSError("gateway closed the connection")
            buf += chunk
            buf = self._drain(buf)

    def _raw_send(self, msg_type, body):
        # Used only for the initial Logon, before `connected` is set —
        # submit_new_order/submit_cancel refuse to send until then.
        self.seq += 1
        header = struct.pack(HEADER_FMT, len(body), MSG_TYPE[msg_type], self.seq)
        with self.send_lock:
            self.sock.sendall(header + body)

    def _drain(self, buf):
        while len(buf) >= HEADER_LEN:
            body_len, msg_type, _seq = struct.unpack(HEADER_FMT, buf[:HEADER_LEN])
            total = HEADER_LEN + body_len
            if len(buf) < total:
                return buf
            body = buf[HEADER_LEN:total]
            buf = buf[total:]
            self._handle(msg_type, body)
        return buf

    def _handle(self, msg_type, body):
        if msg_type == MSG_TYPE["Logon"]:
            self.connected.set()
            print("[bridge] logged on to gateway")
            self._broadcast({"type": "status", "connected": True})
        elif msg_type == MSG_TYPE["ExecutionReport"]:
            client_order_id, exchange_order_id, status, fill_price, fill_qty, leaves_qty = \
                struct.unpack(EXEC_REPORT_FMT, body)
            status_name = EXEC_STATUS.get(status, str(status))
            with self.orders_lock:
                order = self.orders.get(client_order_id)
                if order is None:
                    order = {"client_order_id": client_order_id, "side": "?", "price": 0, "quantity": 0}
                    self.orders[client_order_id] = order
                order["exchange_order_id"] = exchange_order_id
                order["status"] = status_name
                order["fill_price"] = fill_price
                order["fill_quantity"] = fill_qty
                order["leaves_quantity"] = leaves_qty
                order["ts"] = time.time()
                order_copy = dict(order)
            with self.stats_lock:
                if status_name in ("Filled", "PartialFill"):
                    self.stats["fills"] += 1
                elif status_name == "Rejected":
                    self.stats["rejects"] += 1
                elif status_name == "Canceled":
                    self.stats["cancels"] += 1
            self._broadcast({"type": "update", "order": order_copy})


HISTORY_QUERY = """
    SELECT o.seq_num, o.session_id, o.client_order_id, o.side, o.price, o.quantity,
           e.exchange_order_id, e.status, e.fill_price, e.fill_quantity, e.leaves_quantity,
           COALESCE(e.ts_ns, o.ts_ns) AS ts_ns
    FROM orders o
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY exchange_order_id ORDER BY id DESC
        ) AS rn
        FROM execution_reports
        WHERE exchange_order_id != 0
    ) e ON e.exchange_order_id = o.seq_num AND e.rn = 1
    WHERE o.event_type = 1
    ORDER BY o.seq_num DESC
    LIMIT ?;
"""
# Joined on exchange_order_id = orders.seq_num, NOT (session_id,
# client_order_id): session_id is the raw fd (see event.hpp's own
# "wrong long-term" comment), and fds get reused once a connection
# closes. Two different connections that happen to reuse both the same
# fd and the same client_order_id (very plausible — e.g. the bridge's
# own counter always starts at 1) would silently collide under that
# grouping and show one order's fills on another's row. exchange_order_id
# is assigned from seq_num at NewOrder time and is genuinely global for
# the life of the database, so it's the correct join key here.


def query_history(db_path, limit):
    if not os.path.exists(db_path):
        return []
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    try:
        rows = conn.execute(HISTORY_QUERY, (limit,)).fetchall()
    finally:
        conn.close()
    out = []
    for seq_num, session_id, client_order_id, side, price, quantity, \
            exchange_order_id, status, fill_price, fill_quantity, leaves_quantity, ts_ns in rows:
        out.append({
            "seq_num": seq_num, "session_id": session_id, "client_order_id": client_order_id,
            "side": SIDE_NAME.get(side, "?"), "price": price, "quantity": quantity,
            "exchange_order_id": exchange_order_id or 0,
            "status": EXEC_STATUS.get(status, "Submitted") if status is not None else "Submitted",
            "fill_price": fill_price or 0, "fill_quantity": fill_quantity or 0,
            "leaves_quantity": leaves_quantity if leaves_quantity is not None else quantity,
            "ts": (ts_ns or 0) / 1e9,
        })
    return out


def query_stats(db_path):
    """The true global sequence counter, straight from the DB — every
    NewOrder *and* CancelOrder consumes a seq_num (see db_consumer.hpp),
    so this is the one place that reflects the sequencer's real position.
    Acks alone can't reconstruct it in the browser: a CancelOrder's own
    seq_num is never echoed back to the client, only the resolved
    existing order's exchange_order_id is."""
    if not os.path.exists(db_path):
        return {"max_seq": 0, "order_count": 0}
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    try:
        max_seq, count = conn.execute("SELECT COALESCE(MAX(seq_num), 0), COUNT(*) FROM orders;").fetchone()
    finally:
        conn.close()
    return {"max_seq": max_seq, "order_count": count}


class Handler(BaseHTTPRequestHandler):
    link: GatewayLink = None    # set by main() before serving
    db_path: str = None         # set by main() before serving

    def log_message(self, fmt, *args):
        pass  # BaseHTTPRequestHandler logs every request to stderr by default — too noisy for SSE

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path, _, query = self.path.partition("?")
        if path == "/" or path == "/index.html":
            self._serve_file("index.html", "text/html")
        elif path == "/app.js":
            self._serve_file("app.js", "application/javascript")
        elif path == "/events":
            self._serve_sse()
        elif path == "/api/history":
            params = urllib.parse.parse_qs(query)
            limit = min(int(params.get("limit", ["50"])[0]), 500)
            try:
                self._json({"orders": query_history(self.db_path, limit)})
            except sqlite3.Error as e:
                self._json({"error": str(e)}, 500)
        elif path == "/api/stats":
            try:
                self._json(query_stats(self.db_path))
            except sqlite3.Error as e:
                self._json({"error": str(e)}, 500)
        else:
            self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            self._json({"error": "bad json"}, 400)
            return

        if self.path == "/api/order":
            side = payload.get("side")
            price = payload.get("price")
            quantity = payload.get("quantity")
            if side not in SIDE or not isinstance(price, (int, float)) or not isinstance(quantity, (int, float)):
                self._json({"error": "expected {side: buy|sell, price, quantity}"}, 400)
                return
            if not self.link.connected.is_set():
                self._json({"error": "not connected to gateway"}, 503)
                return
            client_order_id = self.link.submit_new_order(side, price, quantity)
            self._json({"client_order_id": client_order_id})
        elif self.path == "/api/cancel":
            client_order_id = payload.get("client_order_id")
            if not isinstance(client_order_id, int):
                self._json({"error": "expected {client_order_id}"}, 400)
                return
            if not self.link.connected.is_set():
                self._json({"error": "not connected to gateway"}, 503)
                return
            self.link.submit_cancel(client_order_id)
            self._json({"ok": True})
        else:
            self.send_error(404)

    def _serve_file(self, name, content_type):
        path = os.path.join(UI_DIR, name)
        try:
            with open(path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        orders, stats = self.link.snapshot()
        self._sse_write({"type": "snapshot", "orders": orders, "stats": stats,
                          "connected": self.link.connected.is_set()})

        q = self.link.subscribe()
        try:
            while True:
                try:
                    msg = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
                    continue
                if msg.get("type") == "update":
                    with self.link.stats_lock:
                        msg = dict(msg, stats=dict(self.link.stats))
                self._sse_write(msg)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.link.unsubscribe(q)

    def _sse_write(self, obj):
        self.wfile.write(f"data: {json.dumps(obj)}\n\n".encode())
        self.wfile.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway-host", default="127.0.0.1")
    ap.add_argument("--gateway-port", type=int, default=9000)
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--http-host", default="127.0.0.1")
    ap.add_argument("--db-path", default="db/hft_lob.db")
    args = ap.parse_args()

    link = GatewayLink(args.gateway_host, args.gateway_port)
    Handler.link = link
    Handler.db_path = args.db_path

    server = ThreadingHTTPServer((args.http_host, args.http_port), Handler)
    print(f"[bridge] UI:      http://{args.http_host}:{args.http_port}/")
    print(f"[bridge] gateway: {args.gateway_host}:{args.gateway_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[bridge] shutting down")


if __name__ == "__main__":
    main()

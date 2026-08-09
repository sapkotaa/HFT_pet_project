#!/usr/bin/env python3
"""Manual end-to-end smoke test for the gateway + sequencer + matching
engine pipeline. Not a unit test — a scripted scenario you read the
output of by eye. Run `./build/gateway 9000` first, then this script.

Exercises: Logon, a resting NewOrder, a crossing NewOrder from a second
connection (expects Filled acks on both sides), a CancelOrder on a
resting order (expects Canceled), and a CancelOrder on an unknown order
(expects Rejected) — matching the verification steps in the matching
engine / WAL implementation plan.
"""
import socket
import struct
import sys
import time

HEADER_FMT = "<HBI"          # body_length, msg_type, seq_num
HEADER_LEN = struct.calcsize(HEADER_FMT)

LOGON_FMT = "<QII"            # client_id, heartbeat_interval_ms, starting_seq_num
NEW_ORDER_FMT = "<Q8sBII"     # client_order_id, symbol[8], side, price, quantity
CANCEL_ORDER_FMT = "<Q"       # client_order_id
EXEC_REPORT_FMT = "<QQBIII"   # client_order_id, exchange_order_id, status, fill_price, fill_qty, leaves_qty

MsgType = {
    "Logon": 1, "Logout": 2, "Heartbeat": 3, "TestRequest": 4, "ResendRequest": 5,
    "NewOrder": 10, "CancelOrder": 11, "ReplaceOrder": 12,
    "ExecutionReport": 20, "Reject": 21,
}
ExecStatus = {1: "Accepted", 2: "Rejected", 3: "Filled", 4: "PartialFill", 5: "Canceled"}
Side = {"Buy": 0, "Sell": 1}


class Client:
    def __init__(self, host, port, client_id):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.client_id = client_id
        self.seq = 0
        self.buf = b""

    def _send(self, msg_type, body):
        self.seq += 1
        header = struct.pack(HEADER_FMT, len(body), MsgType[msg_type], self.seq)
        self.sock.sendall(header + body)

    def logon(self, heartbeat_ms=5000):
        body = struct.pack(LOGON_FMT, self.client_id, heartbeat_ms, self.seq)
        self._send("Logon", body)

    def new_order(self, client_order_id, side, price, quantity, symbol=b"TEST"):
        symbol = symbol.ljust(8, b"\0")[:8]
        body = struct.pack(NEW_ORDER_FMT, client_order_id, symbol, Side[side], price, quantity)
        self._send("NewOrder", body)

    def cancel_order(self, client_order_id):
        body = struct.pack(CANCEL_ORDER_FMT, client_order_id)
        self._send("CancelOrder", body)

    def read_frames(self, duration_s):
        """Drains whatever arrives for `duration_s` seconds, printing
        each ExecutionReport (acks now arrive asynchronously, not on
        the same recv() as the request)."""
        self.sock.settimeout(0.2)
        deadline = time.time() + duration_s
        while time.time() < deadline:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                self.buf += chunk
            except socket.timeout:
                continue
            self._drain_buffer()

    def _drain_buffer(self):
        while len(self.buf) >= HEADER_LEN:
            body_len, msg_type, seq_num = struct.unpack(HEADER_FMT, self.buf[:HEADER_LEN])
            total = HEADER_LEN + body_len
            if len(self.buf) < total:
                return
            body = self.buf[HEADER_LEN:total]
            self.buf = self.buf[total:]
            self._handle(msg_type, seq_num, body)

    def _handle(self, msg_type, seq_num, body):
        if msg_type == MsgType["ExecutionReport"]:
            client_order_id, exchange_order_id, status, fill_price, fill_qty, leaves_qty = \
                struct.unpack(EXEC_REPORT_FMT, body)
            print(f"  [client {self.client_id}] ExecutionReport: "
                  f"client_order_id={client_order_id} exchange_order_id={exchange_order_id} "
                  f"status={ExecStatus.get(status, status)} fill_price={fill_price} "
                  f"fill_qty={fill_qty} leaves_qty={leaves_qty}")
        elif msg_type == MsgType["Logon"]:
            print(f"  [client {self.client_id}] Logon ack")
        else:
            print(f"  [client {self.client_id}] frame type={msg_type} seq={seq_num} len={len(body)}")


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000

    print("== Connecting two clients ==")
    a = Client(host, port, client_id=1)
    b = Client(host, port, client_id=2)
    a.logon()
    b.logon()
    time.sleep(0.2)
    a.read_frames(0.3)
    b.read_frames(0.3)

    print("\n== Client A rests a Buy (client_order_id=1, price=100, qty=10) ==")
    a.new_order(client_order_id=1, side="Buy", price=100, quantity=10)
    a.read_frames(0.5)

    print("\n== Client B crosses with a Sell (client_order_id=1, price=100, qty=10) ==")
    b.new_order(client_order_id=1, side="Sell", price=100, quantity=10)
    a.read_frames(0.5)
    b.read_frames(0.5)

    print("\n== Client A rests another Buy (client_order_id=2, price=90, qty=5), then cancels it ==")
    a.new_order(client_order_id=2, side="Buy", price=90, quantity=5)
    a.read_frames(0.3)
    a.cancel_order(client_order_id=2)
    a.read_frames(0.5)

    print("\n== Client A cancels a nonexistent order (client_order_id=999) — expect Rejected ==")
    a.cancel_order(client_order_id=999)
    a.read_frames(0.5)

    print("\nDone.")


if __name__ == "__main__":
    main()

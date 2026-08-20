#!/usr/bin/env python3
"""
A reference HookRelay webhook receiver.

Two purposes:

1.  A real endpoint to point HookRelay at while developing, and the receiver used by the load
    tests in phase 8.
2.  An *independent* implementation of signature verification. The signing scheme is specified by
    its header format, not by the Java code that happens to produce it, and the only way to know
    that is to implement the other side separately and pin both to the same golden vector. The
    vector below also appears in
    common/src/test/java/com/abhishek/hookrelay/common/webhook/WebhookSignatureTest.java.

It also demonstrates the receiver's half of the delivery contract: HookRelay is at-least-once, so
this deduplicates on X-HookRelay-Delivery-Id, which is stable across retries.

    python3 tools/webhook_receiver.py --secret <secret> [--port 9000]
    python3 tools/webhook_receiver.py --selftest

    --fail-with 500      always respond 500, to exercise retries
    --delay 30           sleep before responding, to exercise timeouts
    --tolerance 300      signature timestamp tolerance in seconds
"""

import argparse
import hashlib
import hmac
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIGNATURE_HEADER = "X-HookRelay-Signature"
DELIVERY_ID_HEADER = "X-HookRelay-Delivery-Id"
EVENT_TYPE_HEADER = "X-HookRelay-Event-Type"
ATTEMPT_HEADER = "X-HookRelay-Attempt"

MAX_BODY_BYTES = 1024 * 1024


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    """HMAC-SHA256 over "<timestamp>.<raw body bytes>", lowercase hex.

    The timestamp is inside the signed string on purpose: without it a captured request would be
    replayable forever, and an attacker could not be stopped by simply checking a header they are
    free to rewrite.
    """
    signed = str(timestamp).encode("ascii") + b"." + body
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def parse_signature_header(value: str):
    """Returns (timestamp, v1_hex) or (None, None) if the header is malformed."""
    if not value:
        return None, None
    timestamp = None
    provided = None
    for part in value.split(","):
        part = part.strip()
        if part.startswith("t="):
            try:
                timestamp = int(part[2:])
            except ValueError:
                return None, None
        elif part.startswith("v1="):
            provided = part[3:]
    return timestamp, provided


def verify(secret: str, header_value: str, body: bytes, tolerance: int = 300) -> tuple[bool, str]:
    timestamp, provided = parse_signature_header(header_value)
    if timestamp is None or provided is None:
        return False, "malformed signature header"

    age = abs(int(time.time()) - timestamp)
    if age > tolerance:
        return False, f"timestamp outside tolerance ({age}s > {tolerance}s)"

    expected = compute_signature(secret, timestamp, body)
    # compare_digest, not ==. A normal comparison returns as soon as two bytes differ, so how long
    # it takes leaks how many leading bytes were correct — enough to recover a signature byte by
    # byte given enough attempts.
    if not hmac.compare_digest(expected, provided):
        return False, "signature mismatch"
    return True, "ok"


class Handler(BaseHTTPRequestHandler):
    secret = ""
    tolerance = 300
    fail_with = None
    delay = 0.0
    seen_delivery_ids: set[str] = set()
    counters = {"received": 0, "verified": 0, "rejected": 0, "duplicates": 0}

    def do_POST(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        length = int(self.headers.get("Content-Length", 0))
        if length > MAX_BODY_BYTES:
            self.send_response(413)
            self.end_headers()
            return
        body = self.rfile.read(length)

        Handler.counters["received"] += 1

        ok, reason = verify(Handler.secret, self.headers.get(SIGNATURE_HEADER), body, Handler.tolerance)
        if not ok:
            Handler.counters["rejected"] += 1
            print(f"REJECTED  {reason}", flush=True)
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"invalid signature")
            return

        Handler.counters["verified"] += 1
        delivery_id = self.headers.get(DELIVERY_ID_HEADER, "")
        attempt = self.headers.get(ATTEMPT_HEADER, "?")
        event_type = self.headers.get(EVENT_TYPE_HEADER, "?")

        # The receiver's half of an at-least-once contract. The delivery id is stable across
        # retries, so a repeat is recognisable and can be dropped instead of processed twice.
        duplicate = delivery_id in Handler.seen_delivery_ids
        if duplicate:
            Handler.counters["duplicates"] += 1
        else:
            Handler.seen_delivery_ids.add(delivery_id)

        try:
            event_id = json.loads(body).get("event_id", "?")
        except (ValueError, AttributeError):
            event_id = "?"

        print(
            f"{'DUPLICATE' if duplicate else 'OK       '} "
            f"delivery={delivery_id} event={event_id} type={event_type} attempt={attempt}",
            flush=True,
        )

        if Handler.delay:
            time.sleep(Handler.delay)

        status = Handler.fail_with or 200
        self.send_response(status)
        self.end_headers()
        self.wfile.write(b"ok" if status < 400 else b"failing on purpose")

    def do_GET(self):  # noqa: N802
        payload = json.dumps(Handler.counters).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass  # the handler prints what matters itself


def selftest() -> int:
    """Pins this implementation to the same golden vector as the Java tests."""
    secret = "whsec_test"
    timestamp = 1700000000
    body = b'{"hello":"world"}'
    expected = "f592bbf3951cfc94e560eecfb5d9dd4da6b0fff2e626235f8ab4b54860925d0b"

    failures = []

    actual = compute_signature(secret, timestamp, body)
    if actual != expected:
        failures.append(f"golden vector mismatch:\n  expected {expected}\n  actual   {actual}")

    header = f"t={timestamp},v1={expected}"
    now = int(time.time())
    # Verified with a huge tolerance because the vector's timestamp is fixed in the past.
    ok, reason = verify(secret, header, body, tolerance=now)
    if not ok:
        failures.append(f"genuine signature did not verify: {reason}")

    ok, _ = verify(secret, header, b'{"hello":"mars"}', tolerance=now)
    if ok:
        failures.append("tampered body verified")

    ok, _ = verify(secret, f"t={timestamp + 10},v1={expected}", body, tolerance=now)
    if ok:
        failures.append("shifted timestamp verified")

    ok, _ = verify("whsec_other", header, body, tolerance=now)
    if ok:
        failures.append("wrong secret verified")

    ok, _ = verify(secret, header, body, tolerance=300)
    if ok:
        failures.append("stale signature verified inside a 300s tolerance")

    for bad in [None, "", "garbage", f"t=abc,v1={expected}", f"t={timestamp}", f"v1={expected}"]:
        ok, _ = verify(secret, bad, body, tolerance=now)
        if ok:
            failures.append(f"malformed header verified: {bad!r}")

    if failures:
        for failure in failures:
            print(f"FAIL  {failure}")
        return 1

    print("selftest OK — matches the golden vector in WebhookSignatureTest")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="HookRelay reference webhook receiver")
    parser.add_argument("--secret", help="endpoint signing secret")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--tolerance", type=int, default=300, help="signature age tolerance, seconds")
    parser.add_argument("--fail-with", type=int, help="always respond with this status")
    parser.add_argument("--delay", type=float, default=0.0, help="seconds to sleep before responding")
    parser.add_argument("--selftest", action="store_true", help="verify against the golden vector and exit")
    args = parser.parse_args()

    if args.selftest:
        return selftest()

    if not args.secret:
        parser.error("--secret is required (or use --selftest)")

    Handler.secret = args.secret
    Handler.tolerance = args.tolerance
    Handler.fail_with = args.fail_with
    Handler.delay = args.delay

    # ThreadingHTTPServer, not HTTPServer. The plain one handles a single request at a time, so a
    # receiver told to delay 300ms caps the ENTIRE system at ~3 deliveries/second no matter how many
    # workers exist — the destination becomes the bottleneck and any throughput or autoscaling
    # measurement taken against it is measuring this server. Found while the phase 8 autoscaling
    # experiment reported 693 of 16,092 deliveries completed in 420 seconds.
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"listening on http://{args.host}:{args.port}  (GET / for counters)", flush=True)
    if args.fail_with:
        print(f"responding {args.fail_with} to every webhook", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n{json.dumps(Handler.counters)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

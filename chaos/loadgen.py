#!/usr/bin/env python3
"""
Constant-rate load against the ingest API, counting exactly what fails.

Runs as a pod inside the cluster and talks to the `api` Service, so it exercises the same path real
traffic takes — including kube-proxy's iptables rules, which is the whole point when measuring what
a rolling deployment costs.

Every request carries a unique Idempotency-Key, so a failure is a genuine failure rather than a
duplicate being deduplicated.

    python3 loadgen.py --url http://api:8080 --duration 60 --concurrency 8

Prints a JSON summary on stdout at the end.
"""

import argparse
import http.client
import json
import sys
import threading
import time
import uuid
from collections import Counter
from urllib.parse import urlparse


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.sent = 0
        self.accepted = 0
        self.failed = 0
        self.by_status = Counter()
        self.by_error = Counter()
        self.latencies_ms = []

    def record(self, status, error, elapsed_ms):
        with self.lock:
            self.sent += 1
            self.latencies_ms.append(elapsed_ms)
            if error is not None:
                self.failed += 1
                self.by_error[error] += 1
            else:
                self.by_status[status] += 1
                # 202 created, 200 duplicate. Anything else during a rollout is a dropped request.
                if status in (200, 202):
                    self.accepted += 1
                else:
                    self.failed += 1


def worker(stop_at, url, tenant, stats, keepalive=True):
    parsed = urlparse(url)
    conn = None

    while time.time() < stop_at:
        started = time.time()
        status = None
        error = None
        try:
            if conn is None:
                # Keep-alive: a fresh connection per request would measure connection setup, and
                # would also mask the failure mode being studied, since a new connection follows a
                # fresh iptables lookup.
                conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=10)

            body = json.dumps({
                "event_type": "payment.succeeded",
                "payload": {"order_id": str(uuid.uuid4())},
            })
            conn.request("POST", "/v1/events", body=body, headers={
                "Content-Type": "application/json",
                "X-Tenant-Id": tenant,
                "Idempotency-Key": str(uuid.uuid4()),
            })
            response = conn.getresponse()
            status = response.status
            response.read()
            if not keepalive:
                conn.close()
                conn = None
        except Exception as exc:  # noqa: BLE001 - every failure mode is interesting here
            error = type(exc).__name__
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

        stats.record(status, error, (time.time() - started) * 1000)

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def percentile(values, pct):
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * pct / 100) - 1))
    return round(ordered[index], 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://api:8080")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--tenant", default=str(uuid.uuid4()))
    # A fresh connection per request means every request does a fresh iptables lookup, which is what
    # exposes the endpoint-propagation race that preStop exists to cover. With keep-alive, a handful
    # of long-lived connections are pinned to pods that shut down gracefully, and the race is barely
    # touched — which is why the first run measured zero failures either way.
    parser.add_argument("--no-keepalive", action="store_true")
    args = parser.parse_args()

    stats = Stats()
    stop_at = time.time() + args.duration
    threads = [
        threading.Thread(target=worker, args=(stop_at, args.url, args.tenant, stats, not args.no_keepalive), daemon=True)
        for _ in range(args.concurrency)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=args.duration + 30)

    summary = {
        "tenant": args.tenant,
        "duration_s": args.duration,
        "concurrency": args.concurrency,
        "keepalive": not args.no_keepalive,
        "sent": stats.sent,
        "accepted": stats.accepted,
        "failed": stats.failed,
        "by_status": dict(stats.by_status),
        "by_error": dict(stats.by_error),
        "rps": round(stats.sent / args.duration, 1),
        "p50_ms": percentile(stats.latencies_ms, 50),
        "p95_ms": percentile(stats.latencies_ms, 95),
        "p99_ms": percentile(stats.latencies_ms, 99),
        "max_ms": round(max(stats.latencies_ms), 1) if stats.latencies_ms else 0,
    }
    print("LOADGEN_RESULT " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())

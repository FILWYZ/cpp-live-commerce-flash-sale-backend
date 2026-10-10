#!/usr/bin/env python3
"""Run isolated Redis/RabbitMQ/MySQL outage and recovery checks."""

import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "deploy/compose/docker-compose.yml"
PROJECT = os.environ.get("ECOMMERCE_FAULT_PROJECT", "live-commerce-fault-" + datetime.now().strftime("%Y%m%d%H%M%S"))
BASE_URL = os.environ.get("DEMO_BASE_URL", "http://127.0.0.1:18084").rstrip("/")
ADMIN_TOKEN = os.environ.get("ECOMMERCE_ADMIN_TOKEN", "dev-admin-token")
PASSWORD = "Fault-Only-Password-2026"

# Supply isolated host ports unless the caller selected its own.
os.environ.setdefault("ECOMMERCE_GATEWAY_PUBLISHED_PORT", BASE_URL.rsplit(":", 1)[-1])
os.environ.setdefault("ECOMMERCE_REDIS_PUBLISHED_PORT", "16384")
os.environ.setdefault("ECOMMERCE_MYSQL_PUBLISHED_PORT", "13321")
os.environ.setdefault("ECOMMERCE_RABBITMQ_PUBLISHED_PORT", "5675")
os.environ.setdefault("ECOMMERCE_RABBITMQ_MANAGEMENT_PUBLISHED_PORT", "15675")
os.environ.setdefault("ECOMMERCE_DEMO_STOCK", "100")
os.environ.setdefault("ECOMMERCE_DEMO_CAMPAIGN_LIMIT", "100")
os.environ.setdefault("ECOMMERCE_FLASH_SALE_USER_RATE_PER_SECOND", "100")
os.environ.setdefault("ECOMMERCE_FLASH_SALE_USER_BURST", "100")


def compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args],
        cwd=ROOT, check=check, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def http(method, path, body=None, token=None, admin=False):
    headers = {}
    data = None
    if body is not None:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    if admin:
        headers["X-Admin-Token"] = ADMIN_TOKEN
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        return error.code, json.loads(raw) if raw else {}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def create_user():
    suffix = uuid.uuid4().hex[:16]
    status, result = http("POST", "/users/register", {"username": "fault_" + suffix, "password": PASSWORD})
    require(status == 201, f"user registration failed: {status} {result}")
    status, result = http("POST", "/users/login", {"username": "fault_" + suffix, "password": PASSWORD})
    require(status == 200, f"user login failed: {status} {result}")
    return result["token"]


def submit(token, request_id):
    return http("POST", "/flash-sale/orders", {
        "promotion_id": "promo-1", "sku_id": "sku-1", "quantity": "1", "idempotency_key": request_id,
    }, token=token)


def wait_request(token, request_id, expected="PENDING_PAYMENT", timeout=25):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status, last = http("GET", "/flash-sale/requests?request_id=" + urllib.parse.quote(request_id), token=token)
        if status == 200 and last.get("state") == expected:
            return last
        time.sleep(0.2)
    raise RuntimeError(f"request {request_id} did not reach {expected}: {status} {last}")


def wait_authenticated(token, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status, last = http("GET", "/orders", token=token)
        if status in (200, 404):
            return
        time.sleep(0.2)
    raise RuntimeError(f"Redis-backed session did not recover: {status} {last}")


def submit_until_accepted(token, request_id, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        status, last = submit(token, request_id)
        if status == 202:
            return status, last
        if status not in (429, 503):
            break
        time.sleep(0.25)
    return status, last


def scalar(sql):
    deadline = time.monotonic() + 20
    last_error = ""
    while time.monotonic() < deadline:
        result = compose("exec", "-T", "mysql", "mysql", "-uroot", "-proot-password", "-Decommerce", "-Nse", sql, check=False)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().splitlines()[-1]
        last_error = result.stdout.strip()
        time.sleep(0.25)
    raise RuntimeError(f"MySQL query did not recover: {sql}: {last_error}")


def container_restart_count(service):
    container_id = compose("ps", "-q", service).stdout.strip()
    require(bool(container_id), f"Compose service {service} has no container")
    result = subprocess.run(["docker", "inspect", "--format", "{{.RestartCount}}", container_id],
                            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return int(result.stdout.strip())


def start_with_crash_hook(service, point, marker_path):
    os.environ["ECOMMERCE_ENABLE_TEST_FAULTS"] = "1"
    os.environ["ECOMMERCE_TEST_CRASH_POINT"] = point
    os.environ["ECOMMERCE_TEST_CRASH_MARKER"] = marker_path
    compose("up", "-d", "--no-deps", "--force-recreate", service)
    time.sleep(2)
    container_id = compose("ps", "-q", service).stdout.strip()
    configured = subprocess.run(["docker", "inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", container_id],
                                check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout
    require("ECOMMERCE_ENABLE_TEST_FAULTS=1" in configured and
            f"ECOMMERCE_TEST_CRASH_POINT={point}" in configured and
            f"ECOMMERCE_TEST_CRASH_MARKER={marker_path}" in configured,
            f"test crash hook environment was not applied to {service}")


def clear_crash_hook(service):
    os.environ["ECOMMERCE_ENABLE_TEST_FAULTS"] = "0"
    os.environ["ECOMMERCE_TEST_CRASH_POINT"] = ""
    os.environ["ECOMMERCE_TEST_CRASH_MARKER"] = ""
    compose("up", "-d", "--no-deps", "--force-recreate", service)
    time.sleep(2)


def wait_for_restart(service, previous_count, timeout=20):
    deadline = time.monotonic() + timeout
    current = previous_count
    while time.monotonic() < deadline:
        current = container_restart_count(service)
        if current > previous_count:
            return current
        time.sleep(0.2)
    container_id = compose("ps", "-q", service).stdout.strip()
    diagnostic = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}} {{.RestartCount}}", container_id],
                                check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout.strip()
    marker = os.environ.get("ECOMMERCE_TEST_CRASH_MARKER", "")
    marker_result = subprocess.run(["docker", "exec", container_id, "sh", "-c", "test -f \"$1\"", "sh", marker],
                                   check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    raise RuntimeError(f"{service} did not restart after the configured one-shot crash (count={current}, "
                       f"state={diagnostic}, marker_created={marker_result.returncode == 0})")


def main():
    ids = []
    summary = {"compose_project": PROJECT, "scenarios": {}}
    try:
        compose("up", "--build", "-d", "--wait", "--wait-timeout", "240")
        token_a = create_user()
        request_a = "fault_" + uuid.uuid4().hex
        ids.append(request_a)

        compose("stop", "redis")
        time.sleep(1)
        status, payload = submit(token_a, request_a)
        require(status == 503, f"Redis outage must fail closed (expected 503, got {status}: {payload})")
        compose("start", "redis")
        compose("up", "-d", "--wait", "--wait-timeout", "120", "redis")
        # Redis holds campaign gate/inventory projections. Reinitialize them
        # from the durable store after a Redis process restart. Restart every
        # Redis client role so no process keeps a pool of pre-restart sockets.
        compose("restart", "api-server")
        compose("up", "-d", "--wait", "--wait-timeout", "120", "api-server")
        compose("restart", "order-consumer", "reconciler")
        time.sleep(2)
        wait_authenticated(token_a)
        campaign_deadline = time.monotonic() + 20
        campaign_status, campaign = 0, {}
        while time.monotonic() < campaign_deadline:
            campaign_status, campaign = http("GET", "/promotions/promo-1")
            if campaign_status == 200 and campaign.get("state") == "RUNNING":
                break
            time.sleep(0.25)
        require(campaign_status == 200 and campaign.get("state") == "RUNNING",
                f"campaign state was not restored after Redis restart: {campaign_status} {campaign}")
        status, before = http("GET", "/stock/sku-1")
        require(status == 200 and before == {"available": 100, "reserved": 0, "sold": 0},
                f"Redis outage changed stock unexpectedly: {status} {before}")
        summary["scenarios"]["redis_unavailable"] = {"http_status": status, "fail_closed": True, "stock_unchanged": True}

        compose("stop", "rabbitmq")
        time.sleep(1)
        status, payload = submit(token_a, request_a)
        require(status == 503, f"RabbitMQ outage must not return 202 (got {status}: {payload})")
        compose("start", "rabbitmq")
        compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
        status, payload = submit_until_accepted(token_a, request_a)
        require(status == 202, f"API publisher did not reconnect after RabbitMQ recovery: {status} {payload}")
        wait_request(token_a, request_a)
        summary["scenarios"]["rabbitmq_unavailable_and_recovery"] = {"outage_status": 503, "recovery_status": 202, "consumer_recovered": True}

        token_precommit = create_user()
        request_precommit = "fault_" + uuid.uuid4().hex
        ids.append(request_precommit)
        start_with_crash_hook("order-consumer", "after_reservation_before_commit",
                              "/tmp/fault-after-reservation.marker")
        status, payload = submit(token_precommit, request_precommit)
        require(status == 202, f"pre-commit crash test admission failed: {status} {payload}")
        precommit_restarts = wait_for_restart("order-consumer", 0)
        clear_crash_hook("order-consumer")
        wait_request(token_precommit, request_precommit)
        summary["scenarios"]["consumer_crash_after_reservation_before_commit"] = {
            "consumer_restart_count": precommit_restarts, "redelivered_and_settled": True,
        }

        token_preack = create_user()
        request_preack = "fault_" + uuid.uuid4().hex
        ids.append(request_preack)
        start_with_crash_hook("order-consumer", "after_commit_before_ack",
                              "/tmp/fault-after-commit.marker")
        status, payload = submit(token_preack, request_preack)
        require(status == 202, f"post-commit crash test admission failed: {status} {payload}")
        preack_restarts = wait_for_restart("order-consumer", 0)
        clear_crash_hook("order-consumer")
        wait_request(token_preack, request_preack)
        summary["scenarios"]["consumer_crash_after_commit_before_ack"] = {
            "consumer_restart_count": preack_restarts, "redelivered_and_settled": True,
        }

        token_b = create_user()
        request_b = "fault_" + uuid.uuid4().hex
        ids.append(request_b)
        compose("stop", "mysql")
        time.sleep(1)
        status, payload = submit(token_b, request_b)
        require(status == 503, f"MySQL outage must not return QUEUED (got {status}: {payload})")
        state_status, outage_state = http("GET", "/flash-sale/requests?request_id=" + request_b, token=token_b)
        require(state_status != 200 or outage_state.get("state") != "PENDING_PAYMENT",
                f"MySQL outage created a false pending-payment order: {outage_state}")
        compose("start", "mysql")
        compose("up", "-d", "--wait", "--wait-timeout", "180", "mysql")
        status, payload = submit_until_accepted(token_b, request_b)
        require(status == 202, f"MySQL client retry after recovery failed: {status} {payload}")
        wait_request(token_b, request_b)
        summary["scenarios"]["mysql_unavailable_and_recovery"] = {
            "outage_status": 503, "recovery_status": 202, "recovery_path": "client retry after MySQL restored",
        }

        token_c = create_user()
        request_c = "fault_" + uuid.uuid4().hex
        ids.append(request_c)
        compose("stop", "reconciler")
        status, payload = submit(token_c, request_c)
        require(status == 202, f"order admission failed with reconciler stopped: {status} {payload}")
        wait_request(token_c, request_c)
        outbox_order_id = payload["order_id"]
        pending_outbox = int(scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_key='" + outbox_order_id + "' AND published=FALSE"))
        require(pending_outbox > 0, "stopping reconciler did not leave an unpublished Outbox event")
        start_with_crash_hook("reconciler", "after_outbox_confirm_before_mark",
                              "/tmp/fault-after-outbox-confirm.marker")
        reconciler_restarts = wait_for_restart("reconciler", 0)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if int(scalar("SELECT COUNT(*) FROM outbox_events WHERE event_key='" + outbox_order_id + "' AND published=FALSE")) == 0:
                break
            time.sleep(0.25)
        remaining_outbox = int(scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_key='" + outbox_order_id + "' AND published=FALSE"))
        attempts = int(scalar("SELECT attempts FROM outbox_events WHERE event_key='" + outbox_order_id + "'"))
        require(remaining_outbox == 0 and attempts >= 2,
                f"Outbox event did not republish and drain after confirm/mark crash: pending={remaining_outbox}, attempts={attempts}")
        summary["scenarios"]["outbox_crash_after_confirm_before_mark"] = {
            "pending_before_restart": pending_outbox, "pending_after_restart": remaining_outbox,
            "attempts": attempts, "reconciler_restart_count": reconciler_restarts,
        }

        escaped = ",".join("'" + value.replace("'", "''") + "'" for value in ids)
        order_count = int(scalar(f"SELECT COUNT(*) FROM orders WHERE idempotency_key IN ({escaped})"))
        require(order_count == 5, f"expected exactly 5 committed orders for accepted recovery requests, got {order_count}")
        status, stock = http("GET", "/stock/sku-1")
        require(status == 200 and stock["available"] + stock["reserved"] + stock["sold"] == 100,
                f"inventory invariant failed: {status} {stock}")
        require(stock["reserved"] == 5 and stock["sold"] == 0,
                f"unexpected final business state: {stock}")
        summary["committed_recovery_orders"] = order_count
        summary["final_stock"] = stock
        print(json.dumps(summary, indent=2, sort_keys=True))
    except Exception:
        try:
            print(compose("logs", "--tail=20", check=False).stdout)
        except Exception:
            pass
        raise
    finally:
        compose("down", "-v", check=False)


if __name__ == "__main__":
    main()

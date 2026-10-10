#!/usr/bin/env python3
"""Run one authenticated flash-sale request against a started demo stack."""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


BASE_URL = os.environ.get("DEMO_BASE_URL", "http://127.0.0.1:8080").rstrip("/")


def request(path, method="GET", body=None, token=None):
    headers = {}
    data = None
    if body is not None:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"body": raw}
        return error.code, payload


def main():
    status, ready = request("/ready")
    if status != 200:
        raise SystemExit(f"Gateway is not ready: HTTP {status} {ready}")

    suffix = uuid.uuid4().hex[:16]
    username = "intern_demo_" + suffix
    password = "Demo-Only-Password-2026"
    status, registered = request("/users/register", "POST", {"username": username, "password": password})
    if status != 201:
        raise SystemExit(f"Registration failed: HTTP {status} {registered}")
    status, login = request("/users/login", "POST", {"username": username, "password": password})
    if status != 200:
        raise SystemExit(f"Login failed: HTTP {status} {login}")

    request_id = "demo-" + suffix
    status, submitted = request("/flash-sale/orders", "POST", {
        "promotion_id": "promo-1",
        "sku_id": "sku-1",
        "quantity": "1",
        "idempotency_key": request_id,
    }, login["token"])
    if status != 202:
        raise SystemExit(f"Flash-sale submission failed: HTTP {status} {submitted}")

    terminal = {"PENDING_PAYMENT", "SOLD_OUT", "REJECTED", "PAID", "EXPIRED", "CANCELLED"}
    deadline = time.monotonic() + 20
    state = submitted
    while time.monotonic() < deadline:
        status, state = request("/flash-sale/requests?request_id=" + urllib.parse.quote(request_id), token=login["token"])
        if status != 200:
            raise SystemExit(f"Request status lookup failed: HTTP {status} {state}")
        if state.get("state") in terminal:
            break
        time.sleep(0.1)
    else:
        raise SystemExit(f"Request did not reach a terminal state: {state}")

    _, stock = request("/stock/sku-1")
    _, stats = request("/debug/stats")
    print(json.dumps({
        "demo": "flash_sale_request",
        "submission_http_status": 202,
        "request_id": request_id,
        "order_id": state.get("order_id"),
        "final_state": state.get("state"),
        "stock": stock,
        "command_queue_depth": stats.get("flash_sale_queue_depth"),
    }, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Fetch the pinned Apache RocketMQ C++ SDK source and its pinned proto API."""

import concurrent.futures
import json
import pathlib
import urllib.request


ROOT = pathlib.Path("/opt/rocketmq-source")
API_REVISION = "bc8e1845a8b0a1b5ec828690324401a323244354"
SPECS = [
    ("apache/rocketmq-clients", "cpp-5.1.1", lambda path: path.startswith("/cpp/") and not path.startswith("/cpp/examples/") and "/tests/" not in path),
    ("apache/rocketmq-apis", API_REVISION, lambda path: path.endswith(".proto") and "/rocketmq/v2/" in path),
]


def fetch(item):
    url, path = item
    path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "live-commerce-sdk-build"})
    with urllib.request.urlopen(request, timeout=60) as response:
        data = response.read()
    path.write_bytes(data)
    return len(data)


def main():
    files = []
    for repository, revision, include in SPECS:
        listing_url = f"https://data.jsdelivr.com/v1/package/gh/{repository}@{revision}/flat"
        with urllib.request.urlopen(listing_url, timeout=60) as response:
            listing = json.load(response)
        base_url = f"https://cdn.jsdelivr.net/gh/{repository}@{revision}"
        for item in listing["files"]:
            name = item["name"]
            if not include(name):
                continue
            relative = pathlib.Path(name.lstrip("/"))
            if repository == "apache/rocketmq-apis":
                relative = pathlib.Path("cpp/proto") / relative
            files.append((base_url + name, ROOT / relative))

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        sizes = list(pool.map(fetch, files))

    proto_dir = ROOT / "cpp/proto/apache/rocketmq/v2"
    proto_dir.mkdir(parents=True, exist_ok=True)
    api_base = f"https://cdn.jsdelivr.net/gh/apache/rocketmq-apis@{API_REVISION}/apache/rocketmq/v2/"
    for name in ("admin.proto", "definition.proto", "service.proto"):
        fetch((api_base + name, proto_dir / name))
    print(f"Fetched {len(files)} source files ({sum(sizes)} bytes); proto revision={API_REVISION}")


if __name__ == "__main__":
    main()

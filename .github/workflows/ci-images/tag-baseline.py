# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read public GHCR manifest digests and guard stable tag writes on workflow retries."""

import argparse
from contextlib import closing
from functools import lru_cache
from http.client import HTTPException, HTTPSConnection
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlencode


DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
ACCEPT = ", ".join(("application/vnd.oci.image.index.v1+json",
                    "application/vnd.docker.distribution.manifest.list.v2+json",
                    "application/vnd.oci.image.manifest.v1+json",
                    "application/vnd.docker.distribution.manifest.v2+json"))


def get(path, headers):
    # Only contact GHCR itself. Never follow a redirect with a registry bearer token.
    with closing(HTTPSConnection("ghcr.io", timeout=15)) as conn:
        conn.request("GET", path, headers=headers)
        response = conn.getresponse()
        return response.status, response.getheader("Docker-Content-Digest"), response.read()


@lru_cache(maxsize=1)
def pull_token(package):
    query = urlencode({"service": "ghcr.io", "scope": f"repository:{package}:pull"})
    status, _, body = get("/token?" + query, {})
    if status != 200:
        raise ValueError(f"anonymous pull token for {package}: HTTP {status}")
    return json.loads(body)["token"]


def manifest_digest(image, tag):
    registry, package = image.split("/", 1)
    if registry != "ghcr.io" or not re.fullmatch(r"[a-z0-9._-]+(?:/[a-z0-9._-]+)+", package):
        raise ValueError(f"invalid GHCR image: {image}")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,127}", tag):
        raise ValueError(f"invalid tag: {tag}")
    headers = {"Authorization": f"Bearer {pull_token(package)}", "Accept": ACCEPT}
    status, digest, body = get(f"/v2/{package}/manifests/{tag}", headers)
    if status == 200 and DIGEST.fullmatch(digest or ""):
        return digest
    if status == 404 and any(err.get("code") == "MANIFEST_UNKNOWN"
                             for err in json.loads(body).get("errors", [])):
        return None
    raise ValueError(f"cannot determine manifest digest for {image}:{tag} (HTTP {status})")


def check(image, tag, desired, records, lookup):
    if not DIGEST.fullmatch(desired):
        raise ValueError(f"invalid candidate digest for {image}:{tag}")
    predecessors = []
    for arch in ("amd64", "arm64"):
        path = records / f"baseline-{arch}.json"
        if not path.exists():
            continue
        record = json.loads(path.read_bytes())
        expected = {"image": image, "platform": f"linux/{arch}", "attempt": "1",
                    "source": os.environ["GITHUB_SHA"], "run": os.environ["GITHUB_RUN_ID"]}
        if any(record.get(k) != v for k, v in expected.items()):
            raise ValueError(f"wrong first-attempt baseline in {path.name} for {image}:{tag}")
        previous = record["previous"][tag]  # A missing key is not a confirmed absent tag.
        if previous is not None and not DIGEST.fullmatch(previous):
            raise ValueError(f"invalid predecessor digest in {path.name} for {image}:{tag}")
        predecessors.append(previous)
    if not predecessors:
        raise ValueError(f"no first-attempt baseline for {image}:{tag}; dispatch this image family")
    current = lookup(tag)
    if current != desired and any(old != current for old in predecessors):
        raise ValueError(f"{image}:{tag} changed since the first attempt (now {current}); "
                         "leaving it untouched. Dispatch this image family for a fresh build")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record = commands.add_parser("record")
    record.add_argument("image")
    record.add_argument("tags", nargs="+")
    verify = commands.add_parser("check")
    verify.add_argument("image")
    verify.add_argument("tag")
    verify.add_argument("digest")
    verify.add_argument("records", type=Path)
    args = parser.parse_args()
    if args.command == "record":
        print(json.dumps({tag: manifest_digest(args.image, tag) for tag in args.tags}))
    else:
        check(args.image, args.tag, args.digest, args.records, lambda tag: manifest_digest(args.image, tag))


if __name__ == "__main__":
    try:
        main()
    except (HTTPException, OSError, ValueError, KeyError, TypeError) as error:
        sys.exit(f"Image tag baseline: {error}")

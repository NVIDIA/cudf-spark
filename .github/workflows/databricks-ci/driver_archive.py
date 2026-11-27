# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stage a pinned distribution on a disposable Databricks driver, never a shared Maven repo."""

from contextlib import redirect_stdout
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile


def copy_verified(source, target, size, digest):
    # Check the bytes we copy, not a shared pathname that another driver can replace.
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as src:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size != size:
            return False
        check = hashlib.sha512()
        remaining = size
        while remaining:
            chunk = src.read(min(remaining, 1024 * 1024))
            if not chunk:
                return False
            check.update(chunk)
            if target is not None:
                target.write(chunk)
            remaining -= len(chunk)
        return not src.read(1) and check.hexdigest() == digest


def _stage(name, entry, local_dir, dbfs_root):
    digest, size, url = entry["sha512"], entry["size"], entry["url"]
    if (not re.fullmatch(r"[\w.-]+\.(?:tar\.gz|tgz)", name) or not re.fullmatch(r"[a-f0-9]{128}", digest)
            or type(size) is not int or size <= 0 or not url.startswith("https://")):
        raise ValueError("Invalid pinned distribution metadata")
    local_dir = Path(local_dir)
    if local_dir.resolve() != local_dir or local_dir.is_relative_to(dbfs_root):
        raise OSError("Expected a private, driver-local cache directory")
    local_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    dest = local_dir / name
    shared = dbfs_root / "cached_jars" / "cudf-spark-ci" / digest / name
    try:
        if copy_verified(dest, None, size, digest):
            print(f"Verified driver-local {name}", flush=True)
            return dest, "driver"
    except FileNotFoundError:
        pass

    with tempfile.TemporaryDirectory(dir=local_dir) as tmp:
        candidate = Path(tmp) / name
        # Only the protected warmer publishes. PR drivers read both namespaces without changing either.
        for source in (shared, dbfs_root / "cached_jars" / name):
            try:
                with candidate.open("wb") as out:
                    hit = copy_verified(source, out, size, digest)
            except OSError:
                continue
            if hit:
                source_label = "dbfs" if source == shared else "legacy-dbfs"
                print(f"Verified {'DBFS' if source == shared else 'legacy DBFS'} cache hit: {name}", flush=True)
                break
        else:
            print(f"Downloading pinned {name}: no verified DBFS hit", flush=True)
            urls = [url]
            if "fallback_url" in entry:
                if not entry["fallback_url"].startswith("https://"):
                    raise ValueError("Invalid fallback distribution URL")
                urls.append(entry["fallback_url"])
            # Reserve part of the original budget for an alternate source even if the first stalls.
            seconds = (300 if size < 16 * 1024 * 1024 else 3600) // len(urls)
            for index, origin in enumerate(urls):
                try:
                    subprocess.run(["curl", "-q", "--fail", "--silent", "--show-error", "--location", "--proto",
                                    "=https", "--proto-redir", "=https", "--connect-timeout", "15", "--max-time",
                                    str(seconds), "--retry", "1", "--max-filesize", str(size), "--output",
                                    str(candidate), origin], check=True, timeout=seconds + 7)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    if index + 1 == len(urls) or getattr(exc, "returncode", None) in (23, 63):
                        # Changing origins cannot fix a local write error or verify oversized bytes.
                        raise
                    print(f"Primary download unavailable; trying pinned fallback for {name}", flush=True)
                    continue
                source_label = "fallback" if index else "primary"
                break
            if not copy_verified(candidate, None, size, digest):
                raise ValueError(f"Downloaded {name} failed size or SHA-512 verification")
        candidate.replace(dest)
    return dest, source_label


def ensure(name, entry, local_dir, dbfs_root=Path("/dbfs")):
    return _stage(name, entry, local_dir, dbfs_root)[0]


def receipt(name, entry, local_dir, dbfs_root=Path("/dbfs")):
    try:
        with redirect_stdout(sys.stderr):
            _, source = _stage(name, entry, local_dir, dbfs_root)
    except subprocess.CalledProcessError as exc:
        print({23: "local-error", 63: "verification-error"}.get(exc.returncode, "download-error"))
        return exc.returncode if exc.returncode > 0 else 128 - exc.returncode
    except subprocess.TimeoutExpired:
        print("download-timeout")
        return 124
    except ValueError:
        print("verification-error")
        return 1
    except OSError:
        print("local-error")
        return 1
    print(source)
    return 0


if __name__ == "__main__":
    args = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
    if sys.argv[4:] == ["--receipt"]:
        sys.exit(receipt(*args))
    ensure(*args)

#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Check the integration image's Python dependencies, Maven, and Java 8 toolchain.
set -euo pipefail
image=${1:?Image reference required}
platform=${2:?Platform required}
case "$platform" in
  linux/amd64) python_platform=linux-x86_64 ;;
  linux/arm64) python_platform=linux-aarch64 ;;
  *) echo "Unsupported smoke platform: $platform" >&2; exit 1 ;;
esac

docker run --rm -i --pull=never --platform "$platform" --network none \
  --entrypoint /bin/bash "$image" -s -- "$python_platform" <<'SMOKE'
set -euo pipefail
installed=$(python -c 'import sysconfig; print(sysconfig.get_platform())')
if [[ "$installed" != "$1" ]]; then
  echo "Expected a $1 Python runtime, found $installed" >&2
  exit 1
fi
# Rocky 9 exposes Java 17 on PATH, but Maven uses Java 8 from JAVA_HOME.
java -version 2>&1 | grep -E '^openjdk version "(1\.8|17)\.'
javac -version 2>&1 | grep -E '^javac (1\.8|17)\.'
if [[ -n ${JAVA_HOME:-} ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi
cd "$(mktemp -d)"
cat > Probe.java <<'JAVA'
class Probe {
  public static void main(String[] args) {
    System.out.println("Java " + System.getProperty("java.version") + " OK");
  }
}
JAVA
javac Probe.java
java -cp . Probe | grep '^Java 1\.8\.'
mvn --version
python -c 'import pandas, pyarrow, pytest, sre_yield, xdist'
SMOKE

#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Check the integration image's Python dependencies, Maven, and opt-in JDK 21.
set -euo pipefail
image=${1:?Image reference required}
platform=${2:?Platform required}
if [[ "$platform" != linux/amd64 ]]; then
  echo "Unsupported smoke platform: $platform (expected linux/amd64)" >&2
  exit 1
fi

docker run --rm -i --pull=never --platform "$platform" --network none \
  --entrypoint /bin/bash "$image" -s <<'SMOKE'
set -euo pipefail
for jdk21 in /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/java-21-openjdk-*; do
  [[ -x "$jdk21/bin/javac" ]] && break
done
for tool in java javac; do
  "$tool" -version
  if [[ $(readlink -f "$(command -v "$tool")") == "$(readlink -f "$jdk21/bin/$tool")" ]]; then
    echo "JDK 21 must be opt-in, but $tool on PATH selects it" >&2
    exit 1
  fi
done
if [[ -n ${JAVA_HOME:-} && $(readlink -f "$JAVA_HOME/bin/java") == "$(readlink -f "$jdk21/bin/java")" ]]; then
  echo "JDK 21 must be opt-in, but JAVA_HOME selects it" >&2
  exit 1
fi
cd "$(mktemp -d)"
cat > Probe.java <<'JAVA'
class Probe {
  public static void main(String[] args) {
    if (Runtime.version().feature() != 21) throw new AssertionError(Runtime.version());
  }
}
JAVA
"$jdk21/bin/javac" --release 21 Probe.java
"$jdk21/bin/java" -cp . Probe
mvn --version
python -c 'import pandas, pyarrow, pytest, sre_yield, xdist'
SMOKE

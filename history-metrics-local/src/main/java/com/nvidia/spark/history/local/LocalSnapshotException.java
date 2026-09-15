/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.history.local;

import java.io.IOException;
import java.util.Objects;

/**
 * Checked failure from an explicit local snapshot operation.
 *
 * <p>{@link #reason()} is the stable branching contract. The inherited message is nonempty redacted
 * diagnostics and the inherited cause is optional; callers must not parse either.
 */
public final class LocalSnapshotException extends IOException {
  private static final long serialVersionUID = 1L;

  /** Closed reason vocabulary for local snapshot failures. */
  public enum Reason {
    /** The operation's relative monotonic budget expired. */
    TIMEOUT,
    /** A zero-budget operation found the normalized path already in use. */
    BUSY,
    /** Filesystem or owned-resource construction failed. */
    IO,
    /** The filesystem does not support the required atomic replacement. */
    ATOMIC_MOVE_UNSUPPORTED,
    /** Snapshot framing, ordering, encoding, or structure is malformed. */
    FORMAT,
    /** Snapshot format or API contract version is incompatible. */
    VERSION,
    /** The checksum detected accidental corruption. */
    INTEGRITY,
    /** A size, count, component, or remaining-section bound was exceeded. */
    BOUNDS,
    /** The supplied governed catalog differs from the snapshot catalog. */
    CATALOG_CONFLICT,
    /** A well-framed declaration or observation contradicts its schema. */
    SCHEMA_CONFLICT,
    /** Restored retention conflicts with the supplied policy envelope. */
    POLICY_CONFLICT
  }

  private final Reason reason;

  LocalSnapshotException(Reason reason, String message) {
    super(requireMessage(message));
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  LocalSnapshotException(Reason reason, String message, Throwable cause) {
    super(requireMessage(message), cause);
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  /**
   * Returns the non-null closed reason on which callers branch.
   *
   * @return snapshot failure reason
   */
  public Reason reason() {
    return reason;
  }

  private static String requireMessage(String message) {
    Objects.requireNonNull(message, "message");
    if (message.isEmpty()) {
      throw new IllegalArgumentException("snapshot diagnostic must not be empty");
    }
    return message;
  }
}

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
package com.nvidia.spark.history;

import java.util.Objects;

/**
 * Machine-readable outcome for summary and backend-write operations.
 *
 * <p>Consumers branch on {@link Code} and treat {@link #reason()} as bounded redacted diagnostics,
 * never as a parsing contract.
 */
public final class Status {
  private static final int MAX_REASON_UTF8_BYTES = 256;
  private static final Status OK_STATUS = new Status(Code.OK, null);

  /** Closed machine-readable summary and write outcome vocabulary. */
  public enum Code {
    /** The operation succeeded; a summary may still be absent. */
    OK,
    /** The requested metric version has no authoritative declaration. */
    NOT_DECLARED,
    /** Client input is malformed or incompatible with the request contract. */
    INVALID_REQUEST,
    /** The relative planning budget expired. */
    DEADLINE_EXCEEDED,
    /** Provider work could not produce a usable result. */
    UNAVAILABLE,
    /** A provider authorization or policy decision denied the operation. */
    DENIED
  }

  private final Code code;
  private final String reason;

  private Status(Code code, String reason) {
    this.code = code;
    this.reason = reason;
  }

  /**
   * Returns the shared successful status, whose reason is null.
   *
   * @return successful status
   */
  public static Status ok() {
    return OK_STATUS;
  }

  /**
   * Creates a non-success status with a required diagnostic reason.
   *
   * @param code any code except {@link Code#OK}
   * @param reason nonempty well-formed strict UTF-8 of at most 256 encoded bytes
   * @return non-success status
   */
  public static Status of(Code code, String reason) {
    Objects.requireNonNull(code, "code");
    if (code == Code.OK) {
      throw new IllegalArgumentException("OK status must be created with Status.ok()");
    }
    return new Status(code, requireReason(reason));
  }

  /**
   * Returns the closed machine-readable outcome.
   *
   * @return status code
   */
  public Code code() {
    return code;
  }

  /**
   * Returns null for {@link Code#OK}, otherwise bounded nonempty diagnostic text.
   *
   * @return nullable diagnostic reason
   */
  public String reason() {
    return reason;
  }

  static String requireReason(String reason) {
    Objects.requireNonNull(reason, "reason");
    if (reason.isEmpty()) {
      throw new IllegalArgumentException("reason must not be empty");
    }
    StrictUtf8.requireEncodedLengthAtMost(reason, "reason", MAX_REASON_UTF8_BYTES);
    return reason;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Status)) {
      return false;
    }
    Status that = (Status) other;
    return code == that.code && Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode() {
    return 31 * code.hashCode() + Objects.hashCode(reason);
  }

  @Override
  public String toString() {
    return "Status{" + "code=" + code + ", reason='" + reason + '\'' + '}';
  }
}

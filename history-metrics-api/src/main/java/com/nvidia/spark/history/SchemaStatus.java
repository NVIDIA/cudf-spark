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
 * Positional outcome of declaring one metric schema.
 *
 * <p>Consumers branch on {@link Code} and never parse reason or warning text.
 */
public final class SchemaStatus {
  /** Closed machine-readable declaration outcome vocabulary. */
  public enum Code {
    /** The declaration is compatible; {@link SchemaStatus#reason()} may contain a warning. */
    ACCEPTED,
    /** The immutable canonical schema conflicts with the established declaration. */
    INCOMPATIBLE,
    /** The declaration input is malformed. */
    INVALID_REQUEST,
    /** Provider work could not produce a usable result. */
    UNAVAILABLE,
    /** A provider authorization or policy decision denied the declaration. */
    DENIED
  }

  private final MetricVersionId metric;
  private final Code code;
  private final String reason;

  private SchemaStatus(MetricVersionId metric, Code code, String reason) {
    this.metric = metric;
    this.code = code;
    this.reason = reason;
  }

  /**
   * Creates an accepted outcome.
   *
   * @param metric non-null declared metric version
   * @param warning optional warning; when present, nonempty strict UTF-8 of at most 256 bytes
   * @return accepted outcome
   */
  public static SchemaStatus accepted(MetricVersionId metric, String warning) {
    Objects.requireNonNull(metric, "metric");
    if (warning != null) {
      Status.requireReason(warning);
    }
    return new SchemaStatus(metric, Code.ACCEPTED, warning);
  }

  /**
   * Creates a non-accepted outcome with a required diagnostic reason.
   *
   * @param metric declared metric version; null only for invalid input with no metric identity
   * @param code any code except {@link Code#ACCEPTED}
   * @param reason nonempty well-formed strict UTF-8 of at most 256 encoded bytes
   * @return non-accepted outcome
   */
  public static SchemaStatus of(MetricVersionId metric, Code code, String reason) {
    Objects.requireNonNull(code, "code");
    if (code == Code.ACCEPTED) {
      throw new IllegalArgumentException(
          "ACCEPTED status must be created with SchemaStatus.accepted()");
    }
    if (metric == null && code != Code.INVALID_REQUEST) {
      throw new NullPointerException("metric");
    }
    return new SchemaStatus(metric, code, Status.requireReason(reason));
  }

  /**
   * Returns the metric version, or null only for an identity-less invalid request.
   *
   * @return nullable metric version
   */
  public MetricVersionId metric() {
    return metric;
  }

  /**
   * Returns the closed machine-readable declaration outcome.
   *
   * @return declaration status code
   */
  public Code code() {
    return code;
  }

  /**
   * Returns an optional accepted warning or a required non-accepted reason.
   *
   * @return null only for an accepted declaration without a warning
   */
  public String reason() {
    return reason;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SchemaStatus)) {
      return false;
    }
    SchemaStatus that = (SchemaStatus) other;
    return Objects.equals(metric, that.metric) &&
        code == that.code &&
        Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(metric);
    result = 31 * result + code.hashCode();
    return 31 * result + Objects.hashCode(reason);
  }

  @Override
  public String toString() {
    return "SchemaStatus{" + "metric=" + metric + ", code=" + code +
        ", reason='" + reason + '\'' + '}';
  }
}

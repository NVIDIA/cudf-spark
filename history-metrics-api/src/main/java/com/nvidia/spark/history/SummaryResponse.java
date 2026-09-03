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
 * Positional fixed-size result of one summary request.
 *
 * <p>An {@code OK} response always has coverage and may have a null summary to represent normal
 * absence of evidence. A non-{@code OK} response has neither summary nor coverage.
 */
public final class SummaryResponse {
  private final Summary summary;
  private final Coverage coverage;
  private final Status status;

  private SummaryResponse(Summary summary, Coverage coverage, Status status) {
    this.summary = summary;
    this.coverage = coverage;
    this.status = status;
  }

  /**
   * Creates a successful response.
   *
   * @param summary evidence aggregate, or null when no relevant evidence exists
   * @param coverage non-null coverage of the requested window
   * @return successful response
   */
  public static SummaryResponse ok(Summary summary, Coverage coverage) {
    return new SummaryResponse(
        summary, Objects.requireNonNull(coverage, "coverage"), Status.ok());
  }

  /**
   * Creates an error response with no summary or coverage.
   *
   * @param status required non-{@code OK} outcome
   * @return error response
   */
  public static SummaryResponse error(Status status) {
    Objects.requireNonNull(status, "status");
    if (status.code() == Status.Code.OK) {
      throw new IllegalArgumentException("an error response requires a non-OK status");
    }
    return new SummaryResponse(null, null, status);
  }

  /**
   * Returns the evidence aggregate, or null for normal absence or an error.
   *
   * @return nullable evidence aggregate
   */
  public Summary summary() {
    return summary;
  }

  /**
   * Returns non-null coverage for {@code OK}, or null for an error.
   *
   * @return nullable coverage
   */
  public Coverage coverage() {
    return coverage;
  }

  /**
   * Returns the non-null machine-readable outcome.
   *
   * @return response status
   */
  public Status status() {
    return status;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SummaryResponse)) {
      return false;
    }
    SummaryResponse that = (SummaryResponse) other;
    return Objects.equals(summary, that.summary) &&
        coverage == that.coverage &&
        status.equals(that.status);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(summary);
    result = 31 * result + Objects.hashCode(coverage);
    return 31 * result + status.hashCode();
  }

  @Override
  public String toString() {
    return "SummaryResponse{" + "summary=" + summary + ", coverage=" + coverage +
        ", status=" + status + '}';
  }
}

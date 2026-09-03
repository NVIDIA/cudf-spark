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
 * Counted synchronous outcome of one backend write batch.
 *
 * <p>The status is a coarse batch outcome and does not expose per-observation reasons.
 */
public final class WriteResult {
  private final int accepted;
  private final int rejected;
  private final Status status;

  private WriteResult(int accepted, int rejected, Status status) {
    this.accepted = accepted;
    this.rejected = rejected;
    this.status = status;
  }

  /**
   * Creates an all-accepted result, including the valid empty-batch {@code 0/0 OK} result.
   *
   * @param accepted nonnegative accepted count
   * @return an {@link Status.Code#OK} result
   */
  public static WriteResult ok(int accepted) {
    return of(accepted, 0, Status.ok());
  }

  /**
   * Creates an all-rejected result.
   *
   * @param rejected positive rejected count
   * @param status a {@code NOT_DECLARED}, {@code INVALID_REQUEST}, or {@code DENIED} status
   * @return an all-rejected result
   */
  public static WriteResult rejected(int rejected, Status status) {
    return of(0, rejected, status);
  }

  /**
   * Creates an unavailable result for a partial, mixed, or storage-failure batch.
   *
   * @param accepted nonnegative accepted count
   * @param rejected positive rejected count
   * @param reason nonempty diagnostic reason of at most 256 strict UTF-8 bytes
   * @return an {@link Status.Code#UNAVAILABLE} result
   */
  public static WriteResult unavailable(int accepted, int rejected, String reason) {
    return of(accepted, rejected, Status.of(Status.Code.UNAVAILABLE, reason));
  }

  /**
   * Creates a validated counted result.
   *
   * <p>{@code OK} requires zero rejected; {@code NOT_DECLARED}, {@code INVALID_REQUEST}, and
   * {@code DENIED} require zero accepted and a positive rejected count; {@code UNAVAILABLE}
   * requires a positive rejected count. Other status codes are not valid write outcomes.
   *
   * @param accepted nonnegative accepted count
   * @param rejected nonnegative rejected count
   * @param status non-null coarse batch status
   * @return the validated result
   * @throws NullPointerException if {@code status} is null
   * @throws IllegalArgumentException if counts and status do not form a valid write outcome
   */
  public static WriteResult of(int accepted, int rejected, Status status) {
    if (accepted < 0 || rejected < 0) {
      throw new IllegalArgumentException("write counts must not be negative");
    }
    Objects.requireNonNull(status, "status");
    switch (status.code()) {
      case OK:
        if (rejected != 0) {
          throw new IllegalArgumentException("OK write result cannot reject observations");
        }
        break;
      case NOT_DECLARED:
      case INVALID_REQUEST:
      case DENIED:
        if (accepted != 0 || rejected == 0) {
          throw new IllegalArgumentException(
              status.code() + " requires zero accepted and a positive rejected count");
        }
        break;
      case UNAVAILABLE:
        if (rejected == 0) {
          throw new IllegalArgumentException(
              "UNAVAILABLE requires at least one rejected observation");
        }
        break;
      default:
        throw new IllegalArgumentException(
            status.code() + " is not a valid backend write status");
    }
    return new WriteResult(accepted, rejected, status);
  }

  /** @return the number of accepted observations */
  public int accepted() {
    return accepted;
  }

  /** @return the number of rejected observations */
  public int rejected() {
    return rejected;
  }

  /** @return the coarse batch status */
  public Status status() {
    return status;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof WriteResult)) {
      return false;
    }
    WriteResult that = (WriteResult) other;
    return accepted == that.accepted &&
        rejected == that.rejected &&
        status.equals(that.status);
  }

  @Override
  public int hashCode() {
    int result = accepted;
    result = 31 * result + rejected;
    return 31 * result + status.hashCode();
  }

  @Override
  public String toString() {
    return "WriteResult{" + "accepted=" + accepted + ", rejected=" + rejected +
        ", status=" + status + '}';
  }
}

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

package com.nvidia.spark.rapids;

/**
 * Thrown by {@link RapidsHostColumnBuilder} when an append would take a column past the size
 * its cuDF representation can hold, before that append writes anything. Earlier appends of the
 * same row, to other columns or to other parts of a nested value, stay until the caller restores
 * a snapshot. Callers that can end the batch early, such as {@code RowToColumnarIterator}, catch
 * it to split; every other caller sees an {@code IllegalArgumentException} with the limit in its
 * message.
 */
public class ColumnLimitExceededException extends IllegalArgumentException {
  private final String limitDetail;

  /**
   * @param limitDetail which limit the column would exceed, and with what size
   * @param remedy what the user can change
   */
  public ColumnLimitExceededException(String limitDetail, String remedy) {
    this(limitDetail, remedy, null);
  }

  public ColumnLimitExceededException(String limitDetail, String remedy, Throwable cause) {
    super(limitDetail + "; " + remedy, cause);
    this.limitDetail = limitDetail;
  }

  /** The limit and the size that would exceed it, without the remedy. */
  public String getLimitDetail() {
    return limitDetail;
  }
}

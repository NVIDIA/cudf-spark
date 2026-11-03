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

import java.io.Serializable;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

public abstract class TypeConverter implements Serializable {
  /**
   * Append row value to the column builder and return the number of data bytes written.
   * The result is a double to allow for fractional bytes (validity bits).
   */
  public abstract double append(SpecializedGetters row, int column, RapidsHostColumnBuilder builder);

  /**
   * Append row value to the column builder, but no value is returned as a performance
   * optimization for scala.
   */
  public void appendNoRet(SpecializedGetters row, int column, RapidsHostColumnBuilder builder) {
    append(row, column, builder);
  }

  /**
   * This is here for structs.  When you append a null to a struct the size is not known
   * ahead of time.  Also because structs push nulls down to the children this size should
   * assume a validity even if the schema says it cannot be null. The result is a double to
   * allow for fractional bytes (validity bits).
   */
  public abstract double getNullSize();
}

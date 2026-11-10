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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;

/** Reflection-only API shapes; none of these methods allocates or invokes native code. */
public final class VariantApiTestFixtures {
  private VariantApiTestFixtures() {}

  public static class LegacyUtils {
    public static ColumnVector getVariantFieldValue(ColumnView input, String path) { return null; }
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
  }

  public abstract static class CloseableResult implements AutoCloseable {
    @Override
    public void close() {}
  }

  public static class Result extends CloseableResult {
    public ColumnVector getValue() { return null; }
    public ColumnVector getStatus() { return null; }
  }

  public static class ExtractionOnlyUtils {
    public static Result getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
  }

  public static class Utils extends ExtractionOnlyUtils {
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) {
      return null;
    }
  }

  public static class WrongExtractionReturnUtils {
    public static Object getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class NonStaticExtractionUtils {
    public Result getVariantFieldValueWithStatus(ColumnView input, String path) { return null; }
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class NonStaticCastUtils extends ExtractionOnlyUtils {
    public Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class WrongCastReturnUtils extends ExtractionOnlyUtils {
    public static Object castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) {
      return null;
    }
  }

  public static class WrongCastParametersUtils extends ExtractionOnlyUtils {
    public static Result castVariantValueWithStatus(ColumnView input, DType type) { return null; }
  }

  public static class NonOwningResult {
    public ColumnVector getValue() { return null; }
    public ColumnVector getStatus() { return null; }
    public void close() {}
  }

  public static class NonOwningUtils {
    public static NonOwningResult getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static NonOwningResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class MissingValueResult extends CloseableResult {
    public ColumnVector getStatus() { return null; }
  }

  public static class MissingValueUtils {
    public static MissingValueResult getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static MissingValueResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class WrongValueResult extends CloseableResult {
    public ColumnView getValue() { return null; }
    public ColumnVector getStatus() { return null; }
  }

  public static class WrongValueUtils {
    public static WrongValueResult getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static WrongValueResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class MissingStatusResult extends CloseableResult {
    public ColumnVector getValue() { return null; }
  }

  public static class MissingStatusUtils {
    public static MissingStatusResult getVariantFieldValueWithStatus(
        ColumnView input, String path) {
      return null;
    }
    public static MissingStatusResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class WrongStatusResult extends CloseableResult {
    public ColumnVector getValue() { return null; }
    public ColumnView getStatus() { return null; }
  }

  public static class WrongStatusUtils {
    public static WrongStatusResult getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static WrongStatusResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class NativeId {
    public int getNativeId() { return 0; }
  }

  public static class Status extends NativeId {
    public static final Status SUCCESS = null;
    public static final Status ROW_NULL = null;
  }

  public static class MissingNativeIdStatus {
    public static final MissingNativeIdStatus SUCCESS = null;
    public static final MissingNativeIdStatus ROW_NULL = null;
  }

  public static class WrongNativeIdStatus {
    public static final WrongNativeIdStatus SUCCESS = null;
    public static final WrongNativeIdStatus ROW_NULL = null;
    public long getNativeId() { return 0; }
  }

  public static class MissingSuccessStatus extends NativeId {
    public static final MissingSuccessStatus ROW_NULL = null;
  }

  public static class WrongSuccessStatus extends NativeId {
    public static final Object SUCCESS = null;
    public static final WrongSuccessStatus ROW_NULL = null;
  }

  public static class MissingRowNullStatus extends NativeId {
    public static final MissingRowNullStatus SUCCESS = null;
  }

  public static class WrongRowNullStatus extends NativeId {
    public static final WrongRowNullStatus SUCCESS = null;
    public static final Object ROW_NULL = null;
  }
}

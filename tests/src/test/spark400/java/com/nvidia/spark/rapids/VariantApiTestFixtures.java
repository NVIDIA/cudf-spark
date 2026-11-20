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

/*** spark-rapids-shim-json-lines
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;

/**
 * Reflection-only API shapes; none of these methods allocates or invokes native code. Each
 * rejected fixture differs from the accepted Utils, Result and Status in one member.
 */
public final class VariantApiTestFixtures {
  private VariantApiTestFixtures() {}

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

  public static class PlainCastUtils extends ExtractionOnlyUtils {
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
  }

  public static class Utils extends PlainCastUtils {
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  // Plain cast variants, each with a valid status cast.

  public static class StatusCastOnlyUtils extends ExtractionOnlyUtils {
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class WrongCastReturnUtils extends StatusCastOnlyUtils {
    public static Object castVariantValue(ColumnView input, DType type) { return null; }
  }

  public static class WrongCastParametersUtils extends StatusCastOnlyUtils {
    public static ColumnVector castVariantValue(ColumnView input) { return null; }
  }

  public static class NonStaticCastUtils extends StatusCastOnlyUtils {
    public ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
  }

  // Status cast variants, each with a valid plain cast.

  public static class WrongStatusCastReturnUtils extends PlainCastUtils {
    public static Object castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class WrongStatusCastParametersUtils extends PlainCastUtils {
    public static Result castVariantValueWithStatus(ColumnView input, DType type) { return null; }
  }

  public static class NonStaticStatusCastUtils extends PlainCastUtils {
    public Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  // Extraction variants, each with valid casts.

  public static class WrongExtractionReturnUtils {
    public static Object getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  public static class NonStaticExtractionUtils {
    public Result getVariantFieldValueWithStatus(ColumnView input, String path) { return null; }
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
    public static Result castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  // Result variants. Their utils return the variant from extraction and the status cast.

  public static class NonOwningResult {
    public ColumnVector getValue() { return null; }
    public ColumnVector getStatus() { return null; }
    public void close() {}
  }

  public static class NonOwningUtils {
    public static NonOwningResult getVariantFieldValueWithStatus(ColumnView input, String path) {
      return null;
    }
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
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
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
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
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
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
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
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
    public static ColumnVector castVariantValue(ColumnView input, DType type) { return null; }
    public static WrongStatusResult castVariantValueWithStatus(
        ColumnView input, DType type, ColumnView prior) { return null; }
  }

  // Status variants.

  public static class NativeId {
    public int getNativeId() { return 0; }
  }

  public static class Status extends NativeId {
    public static final Status VARIANT_NULL = null;
    public static final Status MALFORMED_VARIANT = null;
  }

  public static class MissingNativeIdStatus {
    public static final MissingNativeIdStatus VARIANT_NULL = null;
    public static final MissingNativeIdStatus MALFORMED_VARIANT = null;
  }

  public static class WrongNativeIdStatus {
    public static final WrongNativeIdStatus VARIANT_NULL = null;
    public static final WrongNativeIdStatus MALFORMED_VARIANT = null;
    public long getNativeId() { return 0; }
  }

  public static class MissingVariantNullStatus extends NativeId {
    public static final MissingVariantNullStatus MALFORMED_VARIANT = null;
  }

  public static class WrongVariantNullStatus extends NativeId {
    public static final Object VARIANT_NULL = null;
    public static final WrongVariantNullStatus MALFORMED_VARIANT = null;
  }

  public static class MissingMalformedStatus extends NativeId {
    public static final MissingMalformedStatus VARIANT_NULL = null;
  }

  public static class WrongMalformedStatus extends NativeId {
    public static final WrongMalformedStatus VARIANT_NULL = null;
    public static final Object MALFORMED_VARIANT = null;
  }
}

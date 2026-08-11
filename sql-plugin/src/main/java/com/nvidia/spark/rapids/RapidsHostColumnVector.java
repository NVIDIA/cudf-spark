/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.HashSet;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public final class RapidsHostColumnVector extends RapidsHostColumnVectorCore {

  /**
   * Get the underlying host cudf columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static ai.rapids.cudf.HostColumnVector[] extractBases(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    ai.rapids.cudf.HostColumnVector[] vectors = new ai.rapids.cudf.HostColumnVector[numColumns];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((RapidsHostColumnVector)batch.column(i)).getBase();
    }
    return vectors;
  }

  /**
   * Get the underlying spark compatible host columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static RapidsHostColumnVector[] extractColumns(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    RapidsHostColumnVector[] vectors = new RapidsHostColumnVector[numColumns];

    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((RapidsHostColumnVector)batch.column(i));
    }
    return vectors;
  }

  public static ColumnarBatch incRefCounts(ColumnarBatch batch) {
    for (RapidsHostColumnVector rapidsHostCv: extractColumns(batch)) {
      rapidsHostCv.incRefCount();
    }
    return batch;
  }

  public static long getTotalHostMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() > 0) {
      HashSet<RapidsHostColumnVector> found = new HashSet<>();
      for (RapidsHostColumnVector rapidsHostCv: extractColumns(batch)) {
        if (found.add(rapidsHostCv)) {
          sum += rapidsHostCv.getHostMemoryUsed();
        }
      }
    }
    return sum;
  }

  private final ai.rapids.cudf.HostColumnVector cudfCv;

  /**
   * Sets up the data type of this column vector.
   */
  public RapidsHostColumnVector(DataType type, ai.rapids.cudf.HostColumnVector cudfCv) {
    super(type, cudfCv);
    // TODO need some checks to be sure everything matches
    this.cudfCv = cudfCv;
  }

  public final RapidsHostColumnVector incRefCount() {
    // Just pass through the reference counting
    cudfCv.incRefCount();
    return this;
  }

  public final long getHostMemoryUsed() {
    return cudfCv.getHostMemorySize();
  }

  public final ai.rapids.cudf.HostColumnVector getBase() {
    return cudfCv;
  }
}

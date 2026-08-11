/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
 * Wrapper of a RapidsHostColumnVector, which will check nulls in each "getXXX" call and
 * return the default value of a type when trying to read a null.
 * The performance may not be good enough, so use it only when there is no other way.
 */
public final class RapidsNullSafeHostColumnVector extends RapidsNullSafeHostColumnVectorCore {
  private final RapidsHostColumnVector rapidsHcv;

  public RapidsNullSafeHostColumnVector(RapidsHostColumnVector rapidsHcv) {
    super(rapidsHcv);
    this.rapidsHcv = rapidsHcv;
  }

  public final RapidsNullSafeHostColumnVector incRefCount() {
    // Just pass through the reference counting
    rapidsHcv.incRefCount();
    return this;
  }

  public final ai.rapids.cudf.HostColumnVector getBase() {
    return rapidsHcv.getBase();
  }
}

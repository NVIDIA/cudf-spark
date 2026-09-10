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

package com.nvidia.spark.rapids.iceberg;

/** Root-layout accessors that avoid reflecting the full IcebergProvider interface. */
public final class IcebergProviderAccess {
  private IcebergProviderAccess() {
  }

  public static String detectedVersion() {
    return IcebergProvider$.MODULE$.detectedVersion();
  }

  /**
   * Exposes the selected shim package to the Py4J integration test in
   * {@code iceberg_version_detection_test.py}, which verifies that the detected Iceberg version
   * maps to the expected shim implementation.
   */
  public static String shimPackage() {
    return IcebergProvider$.MODULE$.shimPackage();
  }
}

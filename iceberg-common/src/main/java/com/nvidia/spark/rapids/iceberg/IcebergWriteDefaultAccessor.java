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

import org.apache.iceberg.types.Types;

/**
 * Root-safe bridge for accessing Iceberg write defaults.
 *
 * <p>The default APIs differ across Iceberg versions, so implementations live in the
 * version-specific Iceberg shim modules. Keeping this interface at the JAR root lets shared
 * schema-conversion code use the selected implementation without reflective API access.
 */
public interface IcebergWriteDefaultAccessor {
  boolean hasWriteDefault(Types.NestedField field);

  Object writeDefaultToSpark(Types.NestedField field);
}

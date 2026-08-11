/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, FileSourceMetadataAttribute}

object RapidsFileSourceMetaUtils {
  /**
   * Cleanup the internal metadata information of an attribute if it is
   * a 'FileSourceMetadataAttribute', it will remove both 'METADATA_COL_ATTR_KEY' and
   * 'FILE_SOURCE_METADATA_COL_ATTR_KEY' from the attribute 'Metadata'
   */
  def cleanupFileSourceMetadataInformation(attr: Attribute): Attribute =
    FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation(attr)
}

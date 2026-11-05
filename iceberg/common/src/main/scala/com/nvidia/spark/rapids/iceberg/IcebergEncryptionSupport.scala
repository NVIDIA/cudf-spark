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

package com.nvidia.spark.rapids.iceberg

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsMeta
import org.apache.iceberg.{Table, TableProperties}
import org.apache.iceberg.spark.source.GpuSparkScanAccess

import org.apache.spark.sql.connector.read.Scan

/** Encryption must stay on CPU, including when experimental Iceberg v3 support is enabled. */
object IcebergEncryptionSupport {
  private val UnsupportedReason = "Iceberg encryption is not supported on GPU"
  private val InspectionFailureReason = "Unable to determine Iceberg encryption support"

  def tagForScan(scan: Scan, meta: RapidsMeta[_, _, _]): Unit = {
    tag(meta) {
      GpuSparkScanAccess.table(scan).properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)
    }
  }

  def tagForWrite(table: Table, meta: RapidsMeta[_, _, _]): Unit = {
    tag(meta)(table.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY))
  }

  def tagForWrite(properties: Map[String, String], meta: RapidsMeta[_, _, _]): Unit = {
    tag(meta)(properties.contains(TableProperties.ENCRYPTION_TABLE_KEY))
  }

  private def tag(meta: RapidsMeta[_, _, _])(encrypted: => Boolean): Unit = {
    try {
      if (encrypted) {
        meta.willNotWorkOnGpu(UnsupportedReason)
      }
    } catch {
      // Catalog exceptions can contain sensitive key material.
      // Fail closed without including the exception or any key metadata in the explanation.
      case NonFatal(_) => meta.willNotWorkOnGpu(InspectionFailureReason)
    }
  }
}

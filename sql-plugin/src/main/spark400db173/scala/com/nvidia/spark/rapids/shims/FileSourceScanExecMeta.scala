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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.rapids.ExternalSource

class FileSourceScanExecMeta(plan: FileSourceScanExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends FileSourceScanExecMetaBase(plan, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()
    // DB-17.3 has no Delta provider. If the file format is a ParquetFileFormat subclass
    // (e.g. DeltaParquetFileFormat) not handled by ExternalSource, fall back the scan
    // to CPU so the subclass can handle format-specific features like deletion vectors
    // and skip_row columns.
    val fmtCls = wrapped.relation.fileFormat.getClass
    if (classOf[ParquetFileFormat].isAssignableFrom(fmtCls) &&
        fmtCls != classOf[ParquetFileFormat] &&
        !ExternalSource.isSupportedFormat(fmtCls)) {
      willNotWorkOnGpu(s"unsupported file format: ${fmtCls.getCanonicalName}")
    }
  }
}

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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{BlockInfo, DataBlockBase, ExtraInfo}
import com.nvidia.spark.rapids.{SchemaBase, SingleDataBlockInfo}
import org.apache.avro.Schema
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/** Extra information */
case class AvroExtraInfo() extends ExtraInfo

/** avro schema wrapper */
case class AvroSchemaWrapper(schema: Schema) extends SchemaBase {
  override def isEmpty: Boolean = schema.getFields.isEmpty
}

/** avro BlockInfo wrapper */
case class AvroDataBlock(blockInfo: BlockInfo) extends DataBlockBase {
  override def getRowCount: Long = blockInfo.count
  override def getReadDataSize: Long = blockInfo.dataSize
  override def getBlockSize: Long = blockInfo.blockSize
}

case class AvroSingleDataBlockInfo(
  filePath: Path,
  dataBlock: AvroDataBlock,
  partitionValues: InternalRow,
  schema: AvroSchemaWrapper,
  readSchema: StructType,
  extraInfo: AvroExtraInfo) extends SingleDataBlockInfo

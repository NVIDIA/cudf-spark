/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.GpuUserDefinedFunction
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types.DataType

/** Common implementation across Hive UDFs */
trait GpuHiveUDFBase extends GpuUserDefinedFunction {
  val funcWrapper: HiveFunctionWrapper

  override def nullable: Boolean = true

  override def foldable: Boolean = udfDeterministic && children.forall(_.foldable)

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name
}

/** GPU-accelerated version of Spark's `HiveSimpleUDF` */
case class GpuHiveSimpleUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean) extends GpuHiveUDFBase {
  @scala.annotation.nowarn(
    "msg=class UDF in package exec is deprecated"
  )
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[UDF]().asInstanceOf[RapidsUDF]

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/** GPU-accelerated version of Spark's `HiveGenericUDF` */
case class GpuHiveGenericUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean,
    override val foldable: Boolean) extends GpuHiveUDFBase {
  @transient
  override lazy val function: RapidsUDF = funcWrapper.createFunction[GenericUDF]()
      .asInstanceOf[RapidsUDF]
}

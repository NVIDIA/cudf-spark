/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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


package org.apache.spark.sql.catalyst.expressions.rapids

import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuExpression, GpuOverrides, TypeEnum, TypeSig}

import org.apache.spark.sql.catalyst.expressions.{Expression, GetTimestamp}
import org.apache.spark.sql.rapids.{GpuGetTimestamp, UnixTimeExprMeta}

/**
 * GetTimestamp is marked as private so we had to put it in a place that could access it.
 */
object TimeStamp {

  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[GetTimestamp](
      "Gets timestamps from strings using given pattern.",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[GetTimestamp](a, conf, p, r) {
        override def isTimeZoneSupported = true
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuGetTimestamp(lhs, rhs, sparkFormat, strfFormat, a.timeZoneId)
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
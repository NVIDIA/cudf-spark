/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.TypeSig

object CastCheckShims {

  def typesDayTimeCanCastTo: TypeSig = TypeSig.DAYTIME + TypeSig.STRING + TypeSig.integral

  def typesYearMonthCanCastTo: TypeSig = TypeSig.integral

  def typesDayTimeCanCastToOnSpark: TypeSig = TypeSig.DAYTIME + TypeSig.STRING + TypeSig.integral

  def typesYearMonthCanCastToOnSpark: TypeSig = TypeSig.YEARMONTH + TypeSig.STRING +
    TypeSig.integral

  def additionalTypesBooleanCanCastTo: TypeSig = TypeSig.TIMESTAMP

  def additionalTypesDateCanCastTo: TypeSig = TypeSig.BOOLEAN + TypeSig.integral + TypeSig.fp

  def additionalTypesTimestampCanCastTo: TypeSig = TypeSig.BOOLEAN

  def additionalTypesIntegralCanCastTo: TypeSig = TypeSig.YEARMONTH + TypeSig.DAYTIME

  def additionalTypesStringCanCastTo: TypeSig = TypeSig.DAYTIME
}
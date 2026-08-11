/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.timezone

import scala.util.Random

import org.apache.spark.sql.Column
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class DefaultGeneratorFunction extends GeneratorFunction {
  val random: Random = Random

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    throw new UnsupportedOperationException()

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new UnsupportedOperationException()
}

case class TsGenFunc(values: Array[Long]) extends DefaultGeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    values(random.nextInt(values.length))
  }
}

case class DateGenFunc(values: Array[Int]) extends DefaultGeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    values(random.nextInt(values.length))
  }
}

case class StringGenFunc(strings: Array[String]) extends DefaultGeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    val s = strings(random.nextInt(strings.length))
    UTF8String.fromString(s)
  }
}

object TimeZonePerfUtils {
  def createColumn(idCol: Column, t: DataType, func: GeneratorFunction): Column = {
    val expr = DataGenExprShims.columnToExpr(idCol)
    DataGenExprShims.exprToColumn(DataGenExpr(expr, t, false, func))
  }
}

/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark

import com.nvidia.spark.rapids.Functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.api.java.{UDF0, UDF1, UDF10, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf => sp_udf}
import org.apache.spark.sql.nvidia._
import org.apache.spark.sql.types.LongType

// scalastyle:off
class FunctionsImpl extends Functions {
// scalastyle:on

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function0[Column]): UserDefinedFunction =
    sp_udf(DFUDF0(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function1[Column, Column]): UserDefinedFunction =
    sp_udf(DFUDF1(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function2[Column, Column, Column]): UserDefinedFunction =
    sp_udf(DFUDF2(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function3[Column, Column, Column, Column]): UserDefinedFunction =
    sp_udf(DFUDF3(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function4[Column, Column, Column, Column, Column]): UserDefinedFunction =
    sp_udf(DFUDF4(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function5[Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction = sp_udf(DFUDF5(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function6[Column, Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction = sp_udf(DFUDF6(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function7[Column, Column, Column, Column, Column, Column,
    Column, Column]): UserDefinedFunction = sp_udf(DFUDF7(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function8[Column, Column, Column, Column, Column, Column,
    Column, Column, Column]): UserDefinedFunction = sp_udf(DFUDF8(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function9[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column]): UserDefinedFunction = sp_udf(DFUDF9(f), LongType)

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: Function10[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column, Column]): UserDefinedFunction = sp_udf(DFUDF10(f), LongType)


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Java UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF0[Column]): UserDefinedFunction =
    sp_udf(JDFUDF0(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF1[Column, Column]): UserDefinedFunction =
    sp_udf(JDFUDF1(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF2[Column, Column, Column]): UserDefinedFunction =
    sp_udf(JDFUDF2(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF3[Column, Column, Column, Column]): UserDefinedFunction =
    sp_udf(JDFUDF3(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF4[Column, Column, Column, Column, Column]): UserDefinedFunction =
    sp_udf(JDFUDF4(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF5[Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction = sp_udf(JDFUDF5(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF6[Column, Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction = sp_udf(JDFUDF6(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF7[Column, Column, Column, Column, Column, Column,
    Column, Column]): UserDefinedFunction = sp_udf(JDFUDF7(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF8[Column, Column, Column, Column, Column, Column,
    Column, Column, Column]): UserDefinedFunction = sp_udf(JDFUDF8(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF9[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column]): UserDefinedFunction = sp_udf(JDFUDF9(f), LongType)

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  override def df_udf(f: UDF10[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column, Column]): UserDefinedFunction = sp_udf(JDFUDF10(f), LongType)
}
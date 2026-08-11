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

package com.nvidia.spark.rapids

import org.apache.spark.sql.Column
import org.apache.spark.sql.api.java.{UDF0, UDF1, UDF10, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9}
import org.apache.spark.sql.expressions.UserDefinedFunction

trait Functions {

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function0[Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function1[Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function2[Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function3[Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function4[Column, Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function5[Column, Column, Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function6[Column, Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function7[Column, Column, Column, Column, Column, Column,
    Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function8[Column, Column, Column, Column, Column, Column,
    Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function9[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Scala closure of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to
   * nondeterministic, call the API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: Function10[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column, Column]): UserDefinedFunction


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Java UDF functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF0[Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF1[Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF2[Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF3[Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF4[Column, Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF5[Column, Column, Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF6[Column, Column, Column, Column, Column, Column,
    Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF7[Column, Column, Column, Column, Column, Column,
    Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF8[Column, Column, Column, Column, Column, Column,
    Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF9[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column]): UserDefinedFunction

  /**
   * Defines a Java UDF instance of Columns as user-defined function (UDF).
   * By default the returned UDF is deterministic. To change it to nondeterministic, call the
   * API `UserDefinedFunction.asNondeterministic()`.
   */
  def df_udf(f: UDF10[Column, Column, Column, Column, Column, Column,
    Column, Column, Column, Column, Column]): UserDefinedFunction
}

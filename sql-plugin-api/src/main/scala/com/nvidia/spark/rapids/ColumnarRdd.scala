/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION. All rights reserved.
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

import java.lang.reflect.Method

import ai.rapids.cudf.Table

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * This provides a way to get back out GPU Columnar data RDD[Table]. Each Table will have the same
 * schema as the dataframe passed in.  If the schema of the dataframe is something that Rapids does
 * not currently support an `IllegalArgumentException` will be thrown.
 *
 * The size of each table will be determined by what is producing that table but typically will be
 * about the number of bytes set by [[RapidsConf.GPU_BATCH_SIZE_BYTES]].
 *
 * Table is not a typical thing in an RDD so special care needs to be taken when working with it.
 * By default it is not serializable so repartitioning the RDD or any other operator that involves
 * a shuffle will not work. This is because it is very expensive to serialize and deserialize a GPU
 * Table using a conventional spark shuffle. Also most of the memory associated with the Table is
 * on the GPU itself, so each table must be closed when it is no longer needed to avoid running out
 * of GPU memory.  By convention it is the responsibility of the one consuming the data to close it
 * when they no longer need it.
 */
object ColumnarRdd {

  lazy val convertMethod: Method = ShimLoader.loadColumnarRDD()
    .getDeclaredMethod("convert", classOf[DataFrame])

  def apply(df: DataFrame): RDD[Table] = {
    convert(df)
  }

  def convert(df: DataFrame): RDD[Table] = {
    convertMethod.invoke(null, df).asInstanceOf[RDD[Table]]
  }
}

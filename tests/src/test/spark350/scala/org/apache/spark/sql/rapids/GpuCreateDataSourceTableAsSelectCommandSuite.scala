/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.FunSuiteWithTempDir
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.FileSourceMetadataAttribute.FILE_SOURCE_METADATA_COL_ATTR_KEY
import org.apache.spark.sql.connector.catalog.{Column, Identifier}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class GpuCreateDataSourceTableAsSelectCommandSuite
  extends SparkQueryCompareTestSuite
  with FunSuiteWithTempDir {

  // Fails with Spark < 3.5.0 - see https://github.com/NVIDIA/spark-rapids/issues/8844
  test("Metadata column related field metadata should not be leaked to catalogs") {
    val inputDf = "inputDf"
    val targetTable = "targetTable"
    val columnName = "dataColumn"
    // Create a metadata having an internal metadata field as its key
    val newMetadata = Metadata.fromJson(s"""{"$FILE_SOURCE_METADATA_COL_ATTR_KEY": "dummy"}""")
    withGpuSparkSession { spark =>
      withTable(spark, targetTable) {
        // Create an Dataframe having a column with the above metadata
        val schema = StructType(Array(
          StructField(columnName, StringType, nullable = true, newMetadata)
        ))
        val emptyRDD = spark.sparkContext.emptyRDD[Row]
        spark.createDataFrame(emptyRDD, schema).createOrReplaceTempView(inputDf)

        // Create the target table from the Dataframe (CTAS)
        spark.sql(s"""
          |CREATE TABLE $targetTable USING PARQUET
          |AS SELECT $columnName FROM $inputDf
          |""".stripMargin)

        // Fetch the created table's columns to verify metadata leakage
        val tableColumns = getColumns(spark, targetTable)
        assert(tableColumns.length == 1, "Table should only contain one column.")
        val firstColumn = tableColumns.head
        assert(firstColumn.name == columnName, s"Column name should be '$columnName'.")
        assert(firstColumn.dataType == StringType, "Column type should be StringType.")
        assert(firstColumn.metadataInJSON() == null, "Column metadata should be empty.")
      }
    }
  }

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * This method accesses the current catalog of the Spark session to
   * fetch the schema of the input table. It then returns the columns of the table
   * as an array of Column objects.
   */
  private def getColumns(spark: SparkSession, tableName: String): Array[Column] = {
    val catalogManager = spark.sessionState.catalogManager
    val currentCatalog = catalogManager.currentCatalog.asTableCatalog
    val identifier = Identifier.of(catalogManager.currentNamespace, tableName)
    currentCatalog.loadTable(identifier).columns()
  }
}

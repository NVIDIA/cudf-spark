# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def copy_from_local(spark, local_source, hdfs_target):
    """Copy a local path onto the Hadoop FS used by spark_tmp_path (e.g. GCS on Dataproc)."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    config = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    fs.copyFromLocalFile(Path(local_source), Path(hdfs_target))


def parquet_row_group_midpoints(spark, path):
    """Returns an approximate byte midpoint for each Parquet row group."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
        hadoop_conf, hadoop_path)
    try:
        blocks = reader.getFooter().getBlocks()
        return [
            block.getStartingPos() + block.getCompressedSize() // 2
            for block in blocks
        ]
    finally:
        reader.close()


def assert_parquet_files_use_codec(spark, dataframe, expected_codec):
    file_paths = [row.file for row in
                  dataframe.selectExpr("input_file_name() AS file").distinct().collect()]
    assert file_paths, "No Parquet files found"

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    for file_path in file_paths:
        # input_file_name() returns a URL-encoded URI string; decode it via URI
        reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
            hadoop_conf, jvm.org.apache.hadoop.fs.Path(jvm.java.net.URI(file_path)))
        try:
            row_groups = reader.getFooter().getBlocks()
            assert row_groups.size() > 0, f"Parquet file {file_path} has no row groups"
            for row_group in row_groups:
                columns = row_group.getColumns()
                assert columns.size() > 0, f"Parquet file {file_path} has no column chunks"
                for column in columns:
                    actual_codec = column.getCodec().name()
                    assert actual_codec == expected_codec, \
                        f"Parquet file {file_path} uses {actual_codec}, expected {expected_codec}"
        finally:
            reader.close()

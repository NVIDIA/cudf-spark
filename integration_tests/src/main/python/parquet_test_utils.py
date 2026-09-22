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

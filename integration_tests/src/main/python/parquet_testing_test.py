# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

# Tests based on the Parquet dataset available at
# https://github.com/apache/parquet-testing

import os
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from conftest import get_std_input_path, is_parquet_testing_tests_forced, is_precommit_run, is_not_utc
from data_gen import copy_and_update, non_utc_allow
from marks import allow_non_gpu
from pathlib import Path
import pytest
from spark_session import is_spark_350_or_later, is_spark_411_or_later
import warnings

_rebase_confs = {
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED"
}
_native_reader_confs = copy_and_update(
    _rebase_confs, {"spark.rapids.sql.format.parquet.reader.footer.type": "NATIVE"})
_java_reader_confs = copy_and_update(
    _rebase_confs, {"spark.rapids.sql.format.parquet.reader.footer.type": "JAVA"})

# Basenames of Parquet files that are expected to generate an error mapped to the
# error message. Many of these use "Exception" since the error from libcudf does not
# match the error message from Spark, but the important part is that both CPU and GPU
# agree that the file cannot be loaded.
# When the association is a pair rather than a string, it's a way to xfail the test
# by providing the error string and xfail reason.
_error_files = {
    "large_string_map.brotli.parquet": "Exception",
    "nation.dict-malformed.parquet": "Exception",
    "non_hadoop_lz4_compressed.parquet": "Exception",
    "PARQUET-1481.parquet": "Exception",
}

# Basenames of Parquet files that are expected to fail due to known bugs mapped to the
# xfail reason message.
_xfail_files = {
    "byte_array_decimal.parquet": "https://github.com/NVIDIA/spark-rapids/issues/8629",
    "fixed_length_byte_array.parquet": "https://github.com/rapidsai/cudf/issues/14104",
    "datapage_v2.snappy.parquet": "datapage v2 not supported by cudf",
    "delta_binary_packed.parquet": "https://github.com/rapidsai/cudf/issues/13501",
    "delta_byte_array.parquet": "https://github.com/rapidsai/cudf/issues/13501",
    "delta_encoding_optional_column.parquet": "https://github.com/rapidsai/cudf/issues/13501",
    "delta_encoding_required_column.parquet": "https://github.com/rapidsai/cudf/issues/13501",
    "delta_length_byte_array.parquet": "https://github.com/rapidsai/cudf/issues/13501",
    "hadoop_lz4_compressed.parquet": "cudf does not support Hadoop LZ4 format",
    "hadoop_lz4_compressed_larger.parquet": "cudf does not support Hadoop LZ4 format",
    "nested_structs.rust.parquet": "PySpark cannot handle year 52951",
}
# Spark 3.5.0 adds support for lz4_raw compression codec, but we do not support that on GPU yet
if is_spark_350_or_later():
    _xfail_files["lz4_raw_compressed.parquet"] = "https://github.com/NVIDIA/spark-rapids/issues/9156"
    _xfail_files["lz4_raw_compressed_larger.parquet"] = "https://github.com/NVIDIA/spark-rapids/issues/9156"
else:
    _error_files["lz4_raw_compressed.parquet"] = "Exception"
    _error_files["lz4_raw_compressed_larger.parquet"] = "Exception"

if is_spark_411_or_later():
    # XFAIL null_list.parquet for Spark 4.1.0+ due to https://github.com/NVIDIA/spark-rapids/issues/14242
    # Before Spark 4.1.0, infers array<int>  type for the null_list.parquet,
    # After Spark 4.1.0+, infers array<void> type for the null_list.parquet
    _xfail_files["null_list.parquet"] = "https://github.com/NVIDIA/spark-rapids/issues/14242"

def hdfs_glob(path_str, pattern):
    """
    Finds hdfs files by checking the input path with glob pattern

    :param path_str: hdfs path to check
    :type path_str: str
    :return: generator of matched files
    """
    from spark_init_internal import get_spark_i_know_what_i_am_doing
    full_pattern = os.path.join(path_str, pattern)
    sc = get_spark_i_know_what_i_am_doing().sparkContext
    config = sc._jsc.hadoopConfiguration()
    fs_path = sc._jvm.org.apache.hadoop.fs.Path(full_pattern)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), config)
    statuses = fs.globStatus(fs_path)
    for status in statuses:
        yield status.getPath().toString()

def glob(path_str, pattern):
    """
    Finds files by checking the input path with glob pattern.
    Support local file system and hdfs

    :param path_str: input path to check
    :type path_str: str
    :return: generator of matched files
    """
    if not path_str.startswith('hdfs:'):
        path_list = Path(path_str).glob(pattern)
        return [path.as_posix() for path in path_list]

    return hdfs_glob(path_str, pattern)

def locate_parquet_testing_files():
    """
    Finds the input files by first checking the standard input path,
    falling back to the parquet-testing submodule relative to this
    script's location.
    :param path: standard input path to check
    :return: list of input files or empty list if no files found
    """
    glob_patterns = ("parquet-testing/data/*.parquet", "parquet-testing/bad_data/*.parquet")
    places = []
    std_path = get_std_input_path()
    if std_path: places.append(std_path)
    places.append(Path(__file__).parent.joinpath("../../../../thirdparty").resolve().as_posix())
    for p in places:
        files = []
        for pattern in glob_patterns:
            files += glob(p, pattern)
        if files:
            return files
    locations = ", ".join([os.path.join(p, g) for p in places for g in glob_patterns])
    # TODO: Also fail for nightly tests when nightly scripts have been updated to initialize
    #       the git submodules when pulling spark-rapids changes.
    #       https://github.com/NVIDIA/spark-rapids/issues/8677
    if is_precommit_run() or is_parquet_testing_tests_forced():
        raise AssertionError("Cannot find parquet-testing data in any of: " + locations)
    warnings.warn("Skipping parquet-testing tests. Unable to locate data in any of: " + locations)
    return []

def gen_testing_params_for_errors():
    result = []
    for f in locate_parquet_testing_files():
        error_obj = _error_files.get(os.path.basename(f), None)
        if error_obj is not None:
            result.append((f, error_obj))
    return result

def gen_testing_params_for_valid_files():
    files = []
    for f in locate_parquet_testing_files():
        basename = os.path.basename(f)
        if basename in _error_files:
            continue
        xfail_reason = _xfail_files.get(basename, None)
        if xfail_reason:
            files.append(pytest.param(f, marks=pytest.mark.xfail(reason=xfail_reason)))
        else:
            files.append(f)
    return files

@pytest.mark.parametrize("path", gen_testing_params_for_valid_files())
@pytest.mark.parametrize("confs", [_native_reader_confs, _java_reader_confs])
@allow_non_gpu(*non_utc_allow)
def test_parquet_testing_valid_files(path, confs):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: spark.read.parquet(path), conf=confs)

@pytest.mark.parametrize(("path", "errobj"), gen_testing_params_for_errors())
@pytest.mark.parametrize("confs", [_native_reader_confs, _java_reader_confs])
def test_parquet_testing_error_files(path, errobj, confs):
    error_msg = errobj
    print("error_msg:", error_msg)
    if type(error_msg) != str:
        error_msg, xfail_reason = errobj
        pytest.xfail(xfail_reason)
    assert_gpu_and_cpu_error(
        lambda spark: spark.read.parquet(path).collect(),
        conf=confs,
        error_message=error_msg)

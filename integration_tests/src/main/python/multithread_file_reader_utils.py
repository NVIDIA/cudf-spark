# Copyright (c) 2025, NVIDIA CORPORATION.
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

def resource_bounded_multithreaded_reader_conf(file_type,
                                               specialized_conf = {},
                                               combine_size_conf = None,
                                               keep_order_conf = None,
                                               reader_type_conf = None,
                                               pool_size_conf = None,
                                               memory_limit_conf = None,
                                               timeout_conf = None):
    base_conf = specialized_conf.copy()
    base_conf['spark.rapids.sql.multiThreadedRead.memoryLimit.enabled'] = 'true'
    base_conf['spark.rapids.sql.multiThreadedRead.memoryLimit.tests.perStagePool'] = 'true'
    base_conf['spark.sql.sources.useV1SourceList'] = file_type

    combine_size_val = _list_conf_helper(combine_size_conf, default=[0, 4 << 20])
    combine_size = ('spark.rapids.sql.reader.multithreaded.combine.sizeBytes', combine_size_val)

    keep_order_val = _list_conf_helper(keep_order_conf, default=[False, True])
    keep_order = ('spark.rapids.sql.reader.multithreaded.read.keepOrder', keep_order_val)

    reader_type_val = _list_conf_helper(reader_type_conf, default=['MULTITHREADED', 'COALESCING'])
    reader_type = ('spark.rapids.sql.format.%s.reader.type' % file_type, reader_type_val)

    pool_size_val = _list_conf_helper(pool_size_conf, default=[16, 128])
    pool_size = ('spark.rapids.sql.multiThreadedRead.numThreads', pool_size_val)

    mem_lmt = _list_conf_helper(memory_limit_conf, default=[
        4 << 20,   # 4MB
        32 << 20,   # 32MB
        128 << 20,  # 128MB
    ])
    memory_limit = ('spark.rapids.sql.multiThreadedRead.memoryLimit.size', mem_lmt)

    timeout = _list_conf_helper(timeout_conf, default=[0, 1000, 60000])
    task_timeout = ('spark.rapids.sql.multiThreadedRead.memoryLimit.acquisitionTimeout', timeout)

    conf_matrix = [base_conf]
    for conf_branch in [combine_size, keep_order, reader_type, pool_size, memory_limit, task_timeout]:
        branch_key, branch_values = conf_branch
        updated_matrix = []
        for conf in conf_matrix:
            for value in branch_values:
                conf_copy = conf.copy()
                conf_copy[branch_key] = value
                updated_matrix.append(conf_copy)
        conf_matrix = updated_matrix

    return conf_matrix

def _list_conf_helper(conf, default):
    if conf is None:
        assert isinstance(default, list), "default must be a list: %s" % str(default)
        return default
    elif isinstance(conf, list):
        return conf
    else:
        return [conf]

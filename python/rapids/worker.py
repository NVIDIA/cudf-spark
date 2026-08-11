##
# Copyright (c) 2020-2025, NVIDIA CORPORATION.
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
#
##

import os
import sys
import traceback
from typing import IO, Optional
from pyspark.worker import local_connect_and_auth, main as worker_main
from pyspark.serializers import write_int, write_with_length, SpecialLengths


def initialize_gpu_mem():
    # CUDA device(s) info
    cuda_devices_str = os.environ.get('CUDA_VISIBLE_DEVICES')
    python_gpu_disabled = os.environ.get('RAPIDS_PYTHON_ENABLED', 'false').lower() == 'false'
    if python_gpu_disabled or not cuda_devices_str:
        # Skip gpu initialization due to no CUDA device or python on gpu is disabled.
        # One case to come here is the test runs with cpu session in integration tests.
        return

    print("INFO: Process {} found CUDA visible device(s): {}".format(
        os.getpid(), cuda_devices_str))
    # Initialize RMM only when requiring to enable pooled or managed memory.
    pool_enabled = os.environ.get('RAPIDS_POOLED_MEM_ENABLED', 'false').lower() == 'true'
    uvm_enabled = os.environ.get('RAPIDS_UVM_ENABLED', 'false').lower() == 'true'
    if pool_enabled:
        import rmm
        '''
        RMM will be initialized with default configures (pool disabled) when importing cudf
        as above. So overwrite the initialization when asking for pooled memory,
        along with a pool size and max pool size.
        Meanwhile, the above `import` precedes the `import` in UDF, make our initialization
        not be overwritten again by the `import` in UDF, since Python will ignore duplicated
        `import`.
        '''
        import sys
        max_size = sys.maxint if sys.version_info.major == 2 else sys.maxsize
        pool_size = int(os.environ.get('RAPIDS_POOLED_MEM_SIZE', 0))
        pool_max_size = int(os.environ.get('RAPIDS_POOLED_MEM_MAX_SIZE', 0))
        if 0 < pool_max_size < pool_size:
            raise ValueError("Value of `RAPIDS_POOLED_MEM_MAX_SIZE` should not be less than "
                             "`RAPIDS_POOLED_MEM_SIZE`.")
        if pool_max_size == 0:
            pool_max_size = max_size
        pool_max_size = pool_max_size >> 8 << 8
        pool_size = pool_size >> 8 << 8
        print("DEBUG: Pooled memory, pool size: {} MiB, max size: {} MiB".format(
                pool_size / 1024.0 / 1024,
                ('unlimited' if pool_max_size == max_size else pool_max_size / 1024.0 / 1024)))
        base_t = rmm.mr.ManagedMemoryResource if uvm_enabled else rmm.mr.CudaMemoryResource
        rmm.mr.set_current_device_resource(rmm.mr.PoolMemoryResource(base_t(), pool_size, pool_max_size))
    elif uvm_enabled:
        import rmm
        rmm.mr.set_current_device_resource(rmm.mr.ManagedMemoryResource())
    else:
        # Do nothing, whether to use RMM (default mode) or not depends on UDF definition.
        pass


def handle_worker_exception(
        e: BaseException, outfile: IO, hide_traceback: Optional[bool] = None
) -> None:
    """
    Copied over from pyspark/util.py.

    Handles exception for Python worker which writes SpecialLengths.PYTHON_EXCEPTION_THROWN (-2)
    and exception traceback info to outfile. JVM could then read from the outfile and perform
    exception handling there.

    Parameters
    ----------
    e : BaseException
        Exception handled
    outfile : IO
        IO object to write the exception info
    hide_traceback : bool, optional
        Whether to hide the traceback in the output.
        By default, hides the traceback if environment variable SPARK_HIDE_TRACEBACK is set.
    """

    if hide_traceback is None:
        hide_traceback = bool(os.environ.get("SPARK_HIDE_TRACEBACK", False))

    def format_exception() -> str:
        if hide_traceback:
            return "".join(traceback.format_exception_only(type(e), e))
        if os.environ.get("SPARK_SIMPLIFIED_TRACEBACK", False):
            tb = try_simplify_traceback(sys.exc_info()[-1])  # type: ignore[arg-type]
            if tb is not None:
                e.__cause__ = None
                return "".join(traceback.format_exception(type(e), e, tb))
        return traceback.format_exc()

    try:
        exc_info = format_exception()
        write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
        write_with_length(exc_info.encode("utf-8"), outfile)
    except IOError:
        # JVM close the socket
        pass
    except BaseException:
        # Write the error to stderr if it happened while serializing
        print("PySpark worker failed with exception:", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)


if __name__ == '__main__':
    # Code below is all copied from Pyspark/worker.py
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, sock) = local_connect_and_auth(java_port, auth_secret)
    # Use the `sock_file` as both input and output will cause EOFException in JVM side,
    # So open a new file object on the same socket as output, similar behavior
    # with that in `pyspark/daemon.py`.
    buffer_size = int(os.environ.get("SPARK_BUFFER_SIZE", 65536))
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", buffer_size)
    try:
        # GPU context setup
        initialize_gpu_mem()
        worker_main(sock_file, outfile)
    except BaseException as e:
        handle_worker_exception(e, outfile)
        sys.exit(-1)

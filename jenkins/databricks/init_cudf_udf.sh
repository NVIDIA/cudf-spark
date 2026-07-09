#!/bin/bash
#
# Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
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

# The initscript to set up environment for the cudf_udf tests on Databricks
# Will be automatically pushed into the Databricks Workspace: /databricks/init_scripts/ once it is updated.

set -ex

CUDF_VER=${CUDF_VER:-26.08.0-SNAPSHOT}
CUDA_VER=${CUDA_VER:-12.9}

# Need to explicitly add conda into PATH environment, to activate conda environment.
export PATH=/databricks/conda/bin:$PATH
# Set Python for the running instance
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Rapids 24.10+ drops python 3.9 or below conda packages. ref: https://docs.rapids.ai/notices/rsn0040/
if [[ "$(printf '%s\n' "3.10" "${PYTHON_VERSION}" | sort -V | head -n1)" == "3.10" ]]; then
    # To fix "'lsb_release -a' returned non-zero". ref: https://github.com/pypa/pip/issues/4924
    [[ -n "$(which lsb_release)" ]] && mv $(which lsb_release) $(which lsb_release)"-bak"
else
    echo "Rapids 24.10+ drops python 3.9 or below versions of conda packages"
    exit 255
fi

REQUIRED_PACKAGES=(
  findspark
  pandas
  pyarrow
  pytest
  pytest-order
  pytest-xdist
  requests
  sre_yield
)
if command -v conda >/dev/null 2>&1; then
  base=$(conda info --base)
  # Create and activate 'cudf-udf' conda env for cudf-udf tests
  sudo chmod a+w ${base}/envs && conda config --add envs_dirs ${base}/envs
  conda create -y -n cudf-udf -c conda-forge python=$PYTHON_VERSION mamba && \
    source activate && \
    conda activate cudf-udf

  # Use mamba to install cudf-udf packages to speed up conda resolve time
  conda install -y -c conda-forge mamba python=$PYTHON_VERSION
  # Do not error out "This operation will remove conda without replacing it with another version of conda." for now
  ${base}/envs/cudf-udf/bin/mamba remove -y c-ares zstd libprotobuf pandas || true

  REQUIRED_PACKAGES=(
    "cuda-version=$CUDA_VER"
    "cudf=$CUDF_VER"
    "${REQUIRED_PACKAGES[@]}"
  )

  ${base}/envs/cudf-udf/bin/mamba install -y \
    -c rapidsai -c rapidsai-nightly -c nvidia -c conda-forge -c defaults \
    "${REQUIRED_PACKAGES[@]}"

  source deactivate && conda deactivate
else
  # pip install cudf-py, refer to: https://docs.rapids.ai/install#selector
  # The prefix /databricks/python-bootstrap/ for PYTHON_SITE_PACKAGES is mandatory for Databricks init scripts
  PYTHON_SITE_PACKAGES="/databricks/python-bootstrap/envs/cudf-udf/$PYTHON_VERSION/lib/site-packages"
  pip install --target=${PYTHON_SITE_PACKAGES} \
      --extra-index-url=https://pypi.anaconda.org/rapidsai-wheels-nightly/simple \
      "cudf-cu12>=${CUDF_VER}.0a0,<=${CUDF_VER}"

  REQUIRED_PACKAGES=(
    "${REQUIRED_PACKAGES[@]}"
    scipy
    numexpr
  )
  pip install --target=${PYTHON_SITE_PACKAGES} "${REQUIRED_PACKAGES[@]}"
fi

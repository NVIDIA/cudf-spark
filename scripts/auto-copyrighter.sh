#!/bin/bash

# Copyright (c) 2021, NVIDIA CORPORATION.
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


SPARK_RAPIDS_AUTO_COPYRIGHTER=${SPARK_RAPIDS_AUTO_COPYRIGHTER:-OFF}

case "$SPARK_RAPIDS_AUTO_COPYRIGHTER" in

    OFF)
        echo "Copyright updater is DISABLED. Automatic Copyright Updater can be enabled/disabled by setting \
SPARK_RAPIDS_AUTO_COPYRIGHTER=ON or SPARK_RAPIDS_AUTO_COPYRIGHTER=OFF, \
correspondingly"
        exit 0
        ;;

    ON)
        ;;

    *)
        echo "Invalid value of SPARK_RAPIDS_AUTO_COPYRIGHTER=$SPARK_RAPIDS_AUTO_COPYRIGHTER.
        Only ON or OFF are allowed"
        exit 1
        ;;
esac

set -x
echo "$@" | xargs -L1 sed -i -E \
    "s/Copyright *\(c\) *([0-9,-]+)*-([0-9]{4}), *NVIDIA *CORPORATION/Copyright (c) \\1-`date +%Y`, NVIDIA CORPORATION/; /`date +%Y`/! s/Copyright *\(c\) ([0-9]{4}), *NVIDIA *CORPORATION/Copyright (c) \\1-`date +%Y`, NVIDIA CORPORATION/"

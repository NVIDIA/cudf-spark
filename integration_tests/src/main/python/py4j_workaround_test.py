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

import gc
import logging
import weakref

from spark_init_internal import (
    _apply_py4j_strong_ref_workaround_if_needed,
    get_spark_i_know_what_i_am_doing,
)


class _Container:
    pass


def test_py4j_chained_scala_map_get():
    spark = get_spark_i_know_what_i_am_doing()
    scala_map = spark.conf._jconf.getAll()
    keys = scala_map.keys().iterator()

    assert keys.hasNext()
    key = keys.next()
    assert scala_map.get(key).get() == scala_map.apply(key)


def test_py4j_weak_container_is_replaced_with_strong_reference():
    class WeakJavaMember:
        def __init__(self, name, container, target_id, gateway_client):
            self.container = weakref.ref(container)

    assert _apply_py4j_strong_ref_workaround_if_needed(WeakJavaMember)

    container = _Container()
    container_ref = weakref.ref(container)
    member = WeakJavaMember('get', container, 'o1', None)
    del container
    gc.collect()

    assert container_ref() is member.container


def test_py4j_workaround_is_idempotent():
    class WeakJavaMember:
        def __init__(self, name, container, target_id, gateway_client):
            self.container = weakref.ref(container)

    assert _apply_py4j_strong_ref_workaround_if_needed(WeakJavaMember)
    patched_init = WeakJavaMember.__init__

    assert not _apply_py4j_strong_ref_workaround_if_needed(WeakJavaMember)
    assert WeakJavaMember.__init__ is patched_init


def test_py4j_upstream_strong_container_is_unchanged():
    class StrongJavaMember:
        def __init__(self, name, container, target_id, gateway_client):
            self.container = container

    original_init = StrongJavaMember.__init__

    assert not _apply_py4j_strong_ref_workaround_if_needed(StrongJavaMember)
    assert StrongJavaMember.__init__ is original_init


def test_py4j_probe_failure_skips_workaround(caplog):
    class FailingJavaMember:
        def __init__(self, name, container, target_id, gateway_client):
            raise RuntimeError('probe failed')

    original_init = FailingJavaMember.__init__
    with caplog.at_level(logging.ERROR):
        assert not _apply_py4j_strong_ref_workaround_if_needed(FailingJavaMember)

    assert FailingJavaMember.__init__ is original_init
    assert 'continuing without the temporary integration-test workaround' in caplog.text

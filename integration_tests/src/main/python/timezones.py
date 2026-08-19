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

from conftest import spark_jvm
from dateutil import tz

fixed_offset_timezones = ["Asia/Shanghai", "UTC", "UTC+0", "UTC-0", "GMT", "GMT+0", "GMT-0", "EST", "MST", "VST"]
variable_offset_timezones = ["PST", "NST", "AST", "America/Los_Angeles", "America/New_York", "America/Chicago"]
fixed_offset_timezones_iana = ["Pacific/Pitcairn", "Etc/GMT-0", "Etc/GMT+0", "Asia/Bangkok", "GMT", "MST", "Asia/Calcutta"]
variable_offset_timezones_iana = ["America/Los_Angeles", "America/St_Johns", "America/Halifax", "America/Los_Angeles", "America/New_York", "America/Chicago", "Asia/Kolkata", "Australia/Adelaide", "Pacific/Chatham", "Australia/Lord_Howe"]

# Dynamically get supported timezones from JVM.
# Different JVMs can have different timezones, should not use a constant list here.
# Also different Python versions can have different timezones, so we also filter out timezones not supported by Python.
all_timezones = [zone_name for zone_name in spark_jvm().java.time.ZoneId.getAvailableZoneIds()
                 if tz.gettz(zone_name) is not None]

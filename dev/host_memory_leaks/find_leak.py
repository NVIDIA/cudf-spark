# Copyright (c) 2022, NVIDIA CORPORATION.
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

import sys

def do_it():
    outstanding = {}
    line_num = 0
    for l in sys.stdin:
        line_num = line_num + 1
        line = l.strip()
        if line.startswith("TRACK"):
            parts = line.split("\t")
            if (len(parts) < 3):
                print("PROBLEM ON LINE %s %s"%(line_num, line))
            else:
                address = parts[1]
                func = parts[2]
                if (func == "MALLOC"):
                    if address in outstanding:
                        print("DOUBLE ALLOC: %s"%(address))
                    else:
                        outstanding[address] = line
                elif (func == "FREE"):
                    if address in outstanding:
                        del outstanding[address]
                    else:
                        print("FREE WITHOUT ALLOC: %s"%(address))
                else:
                    print("UNEXPECTED LINE %s"%(line))
    print("LEAKS: %s"%(len(outstanding)))
    for address in outstanding:
        print(outstanding[address])


if __name__ == '__main__':
    do_it()

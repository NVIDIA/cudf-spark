/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.fileio;

import com.nvidia.spark.rapids.jni.fileio.RapidsHostBufferConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;

/** Static helpers for accelerated output-file implementations. */
public final class RapidsOutputFiles {
    private RapidsOutputFiles() {}

    /**
     * Creates a direct host-buffer consumer when PerfIO supports the output configuration.
     * An empty result tells the caller to use the Hadoop output stream.
     */
    public static Optional<RapidsHostBufferConsumer> createHostBufferConsumer(
            Path path, Configuration conf, boolean overwrite) throws IOException {
        return com.nvidia.spark.rapids.PerfIO$.MODULE$.
                createHostBufferConsumer(path, conf, overwrite);
    }
}

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

package com.nvidia.spark.rapids.fileio.hadoop;

import com.nvidia.spark.rapids.fileio.RapidsOutputFiles;
import com.nvidia.spark.rapids.jni.fileio.RapidsHostBufferConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;

/** Hadoop output file with an optional PerfIO direct host-buffer path. */
final class PerfIOOutputFile extends HadoopOutputFile {
    private final Path path;
    private final Configuration conf;

    PerfIOOutputFile(Path path, Configuration conf) throws IOException {
        super(path, path.getFileSystem(conf));
        this.path = path;
        this.conf = conf;
    }

    @Override
    public Optional<RapidsHostBufferConsumer> createHostBufferConsumer(boolean overwrite)
            throws IOException {
        return RapidsOutputFiles.createHostBufferConsumer(path, conf, overwrite);
    }
}

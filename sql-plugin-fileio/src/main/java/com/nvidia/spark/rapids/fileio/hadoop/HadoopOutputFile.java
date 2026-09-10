/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link RapidsOutputFile} using the Hadoop file system.
 * <br/>
 * This class provides methods to create an output file and to obtain the absolute path.
 */
public class HadoopOutputFile implements RapidsOutputFile {
    private final Path filePath;
    private final FileSystem fs;

    public static HadoopOutputFile create(Path filePath, Configuration conf)
        throws IOException {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(conf, "Hadoop conf can't be null");
        FileSystem fs = filePath.getFileSystem(conf);
        return new HadoopOutputFile(filePath, fs);
    }

    private HadoopOutputFile(Path filePath, FileSystem fs) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(fs, "FileSystem can't be null");
        this.filePath = filePath;
        this.fs = fs;
    }

    @Override
    public HadoopOutputStream create(boolean overwrite) throws IOException {
        FSDataOutputStream output = fs.create(filePath, overwrite);
        return new HadoopOutputStream(output);
    }

    @Override
    public String getPath() {
        return filePath.toString();
    }
}

/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.fileio.RapidsInputFiles;
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Implementation {@link RapidsFileIO} using the hadoop file system.
 * <br/>
 */
public class HadoopFileIO implements RapidsFileIO {
    private final SerializableConfiguration hadoopConf;

    public HadoopFileIO(Configuration hadoopConf) {
        Objects.requireNonNull(hadoopConf, "hadoopConf can't be null");
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    @Override
    public RapidsInputFile newInputFile(String path) throws IOException {
        return this.newInputFile(new Path(path));
    }

    @Override
    public RapidsInputFile newInputFile(Path path) throws IOException {
        return newInputFile(path, OptionalLong.empty());
    }

    /**
     * Creates an input file using a file length already known by the caller.
     */
    public RapidsInputFile newInputFile(Path path, long knownLength) throws IOException {
        return newInputFile(path, OptionalLong.of(knownLength));
    }

    private RapidsInputFile newInputFile(Path path, OptionalLong knownLength) throws IOException {
        String scheme = path.toUri().getScheme();
        if (scheme != null && scheme.startsWith("s3") && RapidsInputFiles.isS3PerfEnabled()) {
            return knownLength.isPresent()
                    ? S3InputFile.create(path, hadoopConf.value(), knownLength.getAsLong())
                    : S3InputFile.create(path, hadoopConf.value());
        }
        if (scheme != null && (scheme.equals("gs") || scheme.equals("gcs")) &&
                RapidsInputFiles.isGCSPerfEnabled()) {
            return knownLength.isPresent()
                    ? GCSInputFile.create(path, hadoopConf.value(), knownLength.getAsLong())
                    : GCSInputFile.create(path, hadoopConf.value());
        }
        return knownLength.isPresent()
                ? HadoopInputFile.create(path, hadoopConf.value(), knownLength.getAsLong())
                : HadoopInputFile.create(path, hadoopConf.value());
    }

    @Override
    public HadoopOutputFile newOutputFile(String path) throws IOException {
        Objects.requireNonNull(path, "path can't be null");
        return HadoopOutputFile.create(new Path(path), hadoopConf.value());
    }
}

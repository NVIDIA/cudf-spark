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

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Pluggable factory for {@code RapidsInputFile}, letting an external jar supply an optimized
 * reader (for example with concurrent positional reads in {@code readVectored}) for a Hadoop
 * compatible filesystem that has no built-in optimized implementation.
 * <p>
 * Providers are listed, in priority order, in
 * {@code spark.rapids.sql.inputFileProviderClasses}. For each path the first provider whose
 * {@code supports} returns true creates the input file; when none does, the built-in input
 * files are used. Implementations need a public no-arg constructor and must be thread safe:
 * one instance per class is shared by all reader threads of an executor. A provider owns its
 * own concurrency limits, since many files may already be read in parallel.
 * <p>
 * The provider jar must be on the executor classpath, e.g. via {@code spark.jars}.
 */
public interface RapidsInputFileProvider {
    /**
     * Whether this provider handles the path. Called for every file that is opened, so it must
     * be cheap and do no IO, e.g. a scheme check.
     */
    boolean supports(Path path, Configuration conf);

    /** Creates the input file for a path accepted by {@code supports}. */
    RapidsInputFile create(Path path, Configuration conf) throws IOException;
}

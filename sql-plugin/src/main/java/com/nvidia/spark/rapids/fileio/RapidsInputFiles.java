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

import com.nvidia.spark.rapids.PerfIO;
import com.nvidia.spark.rapids.PerfIOConf;
import org.apache.spark.SparkEnv;

/**
 * Static helpers shared by {@link com.nvidia.spark.rapids.jni.fileio.RapidsInputFile}
 * implementations.
 */
public final class RapidsInputFiles {
    private RapidsInputFiles() {}

    /**
     * Returns the executor-resolved PerfIO S3 enablement. This includes opportunistic
     * enablement when the configuration is unset and the required classes are available.
     */
    public static boolean isS3PerfEnabled() {
        return PerfIO.isS3PerfEnabled();
    }
    /**
     * True iff PerfIO initialized GCS support on this executor. Returns false until
     * PerfIO is initialized.
     */
    public static boolean isGCSPerfEnabled() {
        return PerfIO.isGCSPerfEnabled();
    }

}

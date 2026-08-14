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

/**
 * Static helpers shared by {@link com.nvidia.spark.rapids.jni.fileio.RapidsInputFile}
 * implementations.
 */
public final class RapidsInputFiles {
    private RapidsInputFiles() {}

    /**
     * True iff PerfIO initialized S3 support on this executor. Returns false until
     * PerfIO is initialized.
     */
    public static boolean isS3PerfEnabled() {
        return com.nvidia.spark.rapids.PerfIO$.MODULE$.isS3PerfEnabled();
    }

    /**
     * True iff PerfIO initialized GCS support on this executor. Returns false until
     * PerfIO is initialized.
     */
    public static boolean isGCSPerfEnabled() {
        return com.nvidia.spark.rapids.PerfIO$.MODULE$.isGCSPerfEnabled();
    }

}

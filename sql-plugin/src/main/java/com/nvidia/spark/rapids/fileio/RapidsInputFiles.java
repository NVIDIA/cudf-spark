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

import org.apache.spark.SparkEnv;

/**
 * Static helpers shared by {@link com.nvidia.spark.rapids.jni.fileio.RapidsInputFile}
 * implementations.
 */
public final class RapidsInputFiles {
    private RapidsInputFiles() {}

    private static final String S3_PERF_ENABLED_KEY = "spark.rapids.perfio.s3.enabled";
    private static final String S3_CLIENT_CLASS = "software.amazon.awssdk.services.s3.S3Client";
    private static final String NETTY_CLIENT_CLASS =
            "software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient";
    private static final String CRT_CLIENT_CLASS = "software.amazon.awssdk.crt.s3.S3CrtAsyncClient";

    /**
     * Returns the executor-resolved PerfIO S3 enablement. This includes opportunistic
     * enablement when the configuration is unset and the required classes are available.
     */
    public static boolean isS3PerfEnabled() {
        SparkEnv env = SparkEnv.get();
        if (env == null) {
            return false;
        }
        String configured = env.conf().get(S3_PERF_ENABLED_KEY, null);
        if (configured != null) {
            return Boolean.parseBoolean(configured);
        }
        return hasClass(S3_CLIENT_CLASS) &&
                (hasClass(NETTY_CLIENT_CLASS) || hasClass(CRT_CLIENT_CLASS));
    }

    private static boolean hasClass(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * True iff PerfIO initialized GCS support on this executor. Returns false until
     * PerfIO is initialized.
     */
    public static boolean isGCSPerfEnabled() {
        return com.nvidia.spark.rapids.PerfIO$.MODULE$.isGCSPerfEnabled();
    }

}

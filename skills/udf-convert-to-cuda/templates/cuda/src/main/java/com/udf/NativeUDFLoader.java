/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import ai.rapids.cudf.NativeDepsLoader;

import java.io.IOException;

/** Loads JNI libraries packaged in this UDF jar. */
public final class NativeUDFLoader {
    private static boolean loaded;

    private NativeUDFLoader() {
    }

    public static synchronized void ensureLoaded() {
        if (!loaded) {
            try {
                NativeDepsLoader.loadNativeDeps(new String[] {"rapidsudfjni"});
                loaded = true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to load native CUDA UDF library", e);
            }
        }
    }
}

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

import com.nvidia.spark.rapids.fileio.RapidsInputFileProvider;
import com.nvidia.spark.rapids.fileio.RapidsInputFiles;
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation {@code RapidsFileIO} using the hadoop file system.
 * <br/>
 */
public class HadoopFileIO implements RapidsFileIO {
    /** Comma separated {@code RapidsInputFileProvider} classes, highest priority first. */
    public static final String INPUT_FILE_PROVIDER_CLASSES_KEY =
            "spark.rapids.sql.inputFileProviderClasses";

    // Instantiated providers by the configured class list, shared by all readers of the JVM.
    private static final ConcurrentHashMap<String, List<RapidsInputFileProvider>> PROVIDERS =
            new ConcurrentHashMap<>();

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
        Configuration conf = hadoopConf.value();
        String providerClasses = conf.get(INPUT_FILE_PROVIDER_CLASSES_KEY);
        for (RapidsInputFileProvider provider : providers(providerClasses)) {
            if (provider.supports(path, conf)) {
                return provider.create(path, conf);
            }
        }
        String scheme = path.toUri().getScheme();
        if (scheme != null && scheme.startsWith("s3") && RapidsInputFiles.isS3PerfEnabled()) {
            return S3InputFile.create(path, hadoopConf.value());
        }
        if (scheme != null && (scheme.equals("gs") || scheme.equals("gcs")) &&
                RapidsInputFiles.isGCSPerfEnabled()) {
            return GCSInputFile.create(path, hadoopConf.value());
        }
        return HadoopInputFile.create(path, hadoopConf.value());
    }

    static List<RapidsInputFileProvider> providers(String classNames) {
        if (classNames == null || classNames.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return PROVIDERS.computeIfAbsent(classNames.trim(), HadoopFileIO::loadProviders);
    }

    private static List<RapidsInputFileProvider> loadProviders(String classNames) {
        List<RapidsInputFileProvider> result = new ArrayList<>();
        for (String name : classNames.split(",")) {
            name = name.trim();
            if (!name.isEmpty()) {
                result.add(loadProvider(name));
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static RapidsInputFileProvider loadProvider(String className) {
        Object provider;
        try {
            provider = loadClass(className).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException("Cannot instantiate " + className + " listed in " +
                    INPUT_FILE_PROVIDER_CLASSES_KEY + "; it must be on the executor classpath " +
                    "and have a public no-arg constructor", e);
        }
        if (!(provider instanceof RapidsInputFileProvider)) {
            throw new IllegalArgumentException(className + " listed in " +
                    INPUT_FILE_PROVIDER_CLASSES_KEY + " does not implement " +
                    RapidsInputFileProvider.class.getName());
        }
        return (RapidsInputFileProvider) provider;
    }

    private static Class<?> loadClass(String className) throws ClassNotFoundException {
        // The context class loader sees jars added with spark.jars; the loader of this class
        // covers providers packaged with the plugin itself.
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        if (context != null) {
            try {
                return Class.forName(className, true, context);
            } catch (ClassNotFoundException e) {
                // fall through
            }
        }
        return Class.forName(className, true, HadoopFileIO.class.getClassLoader());
    }

    @Override
    public HadoopOutputFile newOutputFile(String path) throws IOException {
        Objects.requireNonNull(path, "path can't be null");
        return HadoopOutputFile.create(new Path(path), hadoopConf.value());
    }
}

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

import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Serializable snapshot of a session's FileIO settings. */
public final class FileIOConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PREFIX = "spark.sql.rapids.fileio.";
    public static final String IMPL_KEY = PREFIX + "impl";
    // The SQL plugin supplies this implementation and its Hadoop Configuration constructor.
    static final String HADOOP_IMPL = "com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO";

    private final Class<? extends RapidsFileIO> hadoopImpl;
    private final String impl;
    private final Map<String, String> options;

    public FileIOConfig(String impl, Map<String, String> settings,
                        Class<? extends RapidsFileIO> hadoopImpl) {
        this.hadoopImpl = Objects.requireNonNull(hadoopImpl, "hadoopImpl");
        this.impl = Objects.requireNonNull(impl, "impl");
        if (HADOOP_IMPL.equals(impl)) {
            options = Collections.emptyMap();
        } else {
            if (!isResolver(impl)) {
                constructor(impl, null, Map.class, IMPL_KEY);
            }
            options = scoped(settings, PREFIX);
        }
    }

    public RapidsFileIO load(Configuration hadoopConf) {
        return instantiate(impl, options, hadoopConf, hadoopImpl, IMPL_KEY);
    }

    static Map<String, String> scoped(Map<String, String> properties, String prefix) {
        Map<String, String> options = new HashMap<>();
        String selector = prefix + "impl";
        properties.forEach((key, value) -> {
            if (key.startsWith(prefix) && !key.equals(selector)) {
                options.put(key.substring(prefix.length()), value);
            }
        });
        return Collections.unmodifiableMap(options);
    }

    private static boolean isResolver(String name) {
        return name.equals(ResolvingRapidsFileIO.class.getName())
                || name.equals(ResolvingRapidsFileIO.class.getSimpleName());
    }

    private static Constructor<? extends RapidsFileIO> constructor(
            String name, Class<? extends RapidsFileIO> knownClass, Class<?> parameter, String key) {
        try {
            if (knownClass != null) {
                return knownClass.getConstructor(parameter);
            }
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = FileIOConfig.class.getClassLoader();
            }
            return Class.forName(name, false, loader).asSubclass(RapidsFileIO.class)
                    .getConstructor(parameter);
        } catch (ReflectiveOperationException | ClassCastException | LinkageError e) {
            throw new IllegalArgumentException(key + "=" + name
                    + " must implement RapidsFileIO and have a public "
                    + parameter.getName() + " constructor", e);
        }
    }

    static RapidsFileIO instantiate(String name, Map<String, String> options,
                                    Configuration hadoopConf,
                                    Class<? extends RapidsFileIO> hadoopImpl, String key) {
        if (isResolver(name)) {
            return new ResolvingRapidsFileIO(options, hadoopConf, hadoopImpl);
        }
        boolean hadoop = HADOOP_IMPL.equals(name);
        Constructor<? extends RapidsFileIO> ctor =
                constructor(name, hadoop ? hadoopImpl : null,
                        hadoop ? Configuration.class : Map.class, key);
        try {
            return ctor.newInstance(hadoop ? hadoopConf : options);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to initialize " + key + "=" + name,
                    e.getCause());
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new IllegalArgumentException("Failed to instantiate " + key + "=" + name, e);
        }
    }
}

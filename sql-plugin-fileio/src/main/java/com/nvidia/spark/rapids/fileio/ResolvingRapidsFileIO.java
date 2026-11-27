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
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.RapidsOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Chooses a FileIO when an input or output file is created. */
public final class ResolvingRapidsFileIO implements RapidsFileIO {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> options;
    private final SerializableConfiguration hadoopConf;
    private final URI defaultUri;
    private final Class<? extends RapidsFileIO> hadoopImpl;
    private transient Map<String, RapidsFileIO> delegates = new ConcurrentHashMap<>();

    public ResolvingRapidsFileIO(Map<String, String> options) {
        this(options, new Configuration(), null);
    }

    ResolvingRapidsFileIO(Map<String, String> options, Configuration conf,
                          Class<? extends RapidsFileIO> hadoopImpl) {
        this.hadoopImpl = hadoopImpl;
        this.options = Collections.unmodifiableMap(new HashMap<>(options));
        this.hadoopConf = new SerializableConfiguration(conf);
        this.defaultUri = FileSystem.getDefaultUri(conf);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        delegates = new ConcurrentHashMap<>();
    }

    RapidsFileIO delegateFor(Path path) {
        String scheme = path.toUri().getScheme();
        if (scheme == null) {
            scheme = defaultUri.getScheme();
        }
        String scope = scheme.toLowerCase(Locale.ROOT) + ".";
        String selector = scope + "impl";
        String name = options.get(selector);
        if (name == null) {
            // The empty scope shares one Hadoop fallback across all unconfigured schemes.
            return delegates.computeIfAbsent("", ignored -> FileIOConfig.instantiate(
                    FileIOConfig.HADOOP_IMPL, Collections.emptyMap(), hadoopConf.value(), hadoopImpl,
                    FileIOConfig.IMPL_KEY));
        }
        return delegates.computeIfAbsent(scope, ignored -> FileIOConfig.instantiate(
                name, FileIOConfig.scoped(options, scope), hadoopConf.value(), hadoopImpl,
                FileIOConfig.PREFIX + selector));
    }

    private Path qualify(Path path) throws IOException {
        if (path.toUri().getScheme() != null) {
            return path;
        }
        if (path.isAbsolute()) {
            return path.makeQualified(defaultUri, path);
        }
        return FileSystem.get(hadoopConf.value()).makeQualified(path);
    }

    @Override
    public RapidsInputFile newInputFile(Path path) throws IOException {
        return delegateFor(path).newInputFile(qualify(path));
    }

    @Override
    public RapidsInputFile newInputFile(String path) throws IOException {
        Path location = new Path(path);
        return location.toUri().getScheme() == null ? newInputFile(location)
                : delegateFor(location).newInputFile(path);
    }

    @Override
    public RapidsOutputFile newOutputFile(Path path) throws IOException {
        return delegateFor(path).newOutputFile(qualify(path));
    }

    @Override
    public RapidsOutputFile newOutputFile(String path) throws IOException {
        Path location = new Path(path);
        return location.toUri().getScheme() == null ? newOutputFile(location)
                : delegateFor(location).newOutputFile(path);
    }
}

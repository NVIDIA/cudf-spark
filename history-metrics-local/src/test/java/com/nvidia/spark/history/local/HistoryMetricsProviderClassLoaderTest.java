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
package com.nvidia.spark.history.local;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.ServiceLoader;

import com.nvidia.spark.history.HistoryMetricsProvider;
import com.nvidia.spark.history.MetricStore;
import com.nvidia.spark.history.MetricStores;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HistoryMetricsProviderClassLoaderTest {
  @TempDir
  Path temporaryDirectory;

  @Test
  void shimChildDiscoversProviderFromApplicationParent() throws Exception {
    String serviceName = HistoryMetricsProvider.class.getName();
    Path serviceFile = temporaryDirectory.resolve("META-INF/services").resolve(serviceName);
    Files.createDirectories(serviceFile.getParent());
    Files.write(serviceFile, (ExternalProvider.class.getName() + "\n").getBytes(UTF_8));

    URL testClasses = ExternalProvider.class.getProtectionDomain()
        .getCodeSource()
        .getLocation();
    ClassLoader apiLoader = HistoryMetricsProvider.class.getClassLoader();
    try (ProviderClassLoader applicationLoader = new ProviderClassLoader(
            new URL[] {temporaryDirectory.toUri().toURL(), testClasses},
            apiLoader,
            ExternalProvider.class.getName());
        URLClassLoader shimLoader = new URLClassLoader(new URL[0], applicationLoader)) {
      Iterator<HistoryMetricsProvider> providers =
          ServiceLoader.load(HistoryMetricsProvider.class, shimLoader).iterator();

      HistoryMetricsProvider provider = null;
      while (providers.hasNext() && provider == null) {
        HistoryMetricsProvider candidate = providers.next();
        if ("external-test".equals(candidate.name())) {
          provider = candidate;
        }
      }

      assertNotNull(provider);
      assertEquals("external-test", provider.name());
      assertSame(applicationLoader, provider.getClass().getClassLoader());
      assertSame(HistoryMetricsProvider.class, provider.getClass().getInterfaces()[0]);
    }
  }

  public static final class ExternalProvider implements HistoryMetricsProvider {
    @Override
    public String name() {
      return "external-test";
    }

    @Override
    public MetricStore open(SparkContext sparkContext) {
      return MetricStores.current();
    }

    @Override
    public boolean shutdown(Duration timeout) {
      return true;
    }
  }

  private static final class ProviderClassLoader extends URLClassLoader {
    private final String providerClassName;

    private ProviderClassLoader(
        URL[] urls,
        ClassLoader parent,
        String providerClassName) {
      super(urls, parent);
      this.providerClassName = providerClassName;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!providerClassName.equals(name)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = findClass(name);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }
}

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
package com.nvidia.spark.history;

import java.util.Objects;

/**
 * Immutable framework-owned diagnostic provenance attached before a backend write.
 *
 * <p>Text must already be redacted by the caller. Validation enforces encoding and bounds; it does
 * not discover secrets or make the values an authentication boundary. {@code writtenAtMs} is
 * diagnostic only and does not control planning visibility, retention, or selection.
 */
public final class Provenance {
  private static final int MAX_APP_BYTES = 255;
  private static final int MAX_ATTEMPT_BYTES = 64;
  private static final int MAX_PLUGIN_VERSION_BYTES = 64;

  private final String app;
  private final String attempt;
  private final String pluginVersion;
  private final long writtenAtMs;

  /**
   * Creates bounded diagnostic provenance.
   *
   * @param app nonempty redacted application identity, at most 255 strict UTF-8 bytes
   * @param attempt optional redacted attempt identity, at most 64 strict UTF-8 bytes
   * @param pluginVersion nonempty redacted producer version, at most 64 strict UTF-8 bytes
   * @param writtenAtMs framework write time in epoch milliseconds
   * @throws NullPointerException if {@code app} or {@code pluginVersion} is null
   * @throws IllegalArgumentException if required text is empty, malformed, or over its byte bound
   */
  public Provenance(String app, String attempt, String pluginVersion, long writtenAtMs) {
    this.app = validateRequired(app, "app", MAX_APP_BYTES);
    this.attempt = attempt == null ? null :
        validateOptional(attempt, "attempt", MAX_ATTEMPT_BYTES);
    this.pluginVersion =
        validateRequired(pluginVersion, "pluginVersion", MAX_PLUGIN_VERSION_BYTES);
    this.writtenAtMs = writtenAtMs;
  }

  /** @return the nonempty redacted application identity */
  public String app() {
    return app;
  }

  /** @return the redacted attempt identity, or {@code null} when absent */
  public String attempt() {
    return attempt;
  }

  /** @return the nonempty redacted producer version */
  public String pluginVersion() {
    return pluginVersion;
  }

  /** @return the diagnostic framework write time in epoch milliseconds */
  public long writtenAtMs() {
    return writtenAtMs;
  }

  private static String validateRequired(String value, String name, int maxBytes) {
    Objects.requireNonNull(value, name);
    if (value.isEmpty()) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return validateOptional(value, name, maxBytes);
  }

  private static String validateOptional(String value, String name, int maxBytes) {
    if (StrictUtf8.encode(value, name).length > maxBytes) {
      throw new IllegalArgumentException(name + " exceeds " + maxBytes + " UTF-8 bytes");
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Provenance)) {
      return false;
    }
    Provenance that = (Provenance) other;
    return writtenAtMs == that.writtenAtMs &&
        app.equals(that.app) &&
        Objects.equals(attempt, that.attempt) &&
        pluginVersion.equals(that.pluginVersion);
  }

  @Override
  public int hashCode() {
    int result = app.hashCode();
    result = 31 * result + Objects.hashCode(attempt);
    result = 31 * result + pluginVersion.hashCode();
    return 31 * result + (int) (writtenAtMs ^ (writtenAtMs >>> 32));
  }

  @Override
  public String toString() {
    return "Provenance{" + "app='" + app + '\'' + ", attempt='" + attempt + '\'' +
        ", pluginVersion='" + pluginVersion + '\'' + ", writtenAtMs=" + writtenAtMs + '}';
  }
}

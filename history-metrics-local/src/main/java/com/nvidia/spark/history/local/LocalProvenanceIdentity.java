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

import java.util.Objects;

import com.nvidia.spark.history.Provenance;

/**
 * Immutable caller-redacted identity for local framework provenance.
 *
 * <p>The caller must supply redacted diagnostic text. This value validates encoding and bounds but
 * does not classify arbitrary text as a credential or secret. The optional attempt ID is the sole
 * nullable field.
 */
public final class LocalProvenanceIdentity {
  private final String applicationId;
  private final String attemptId;
  private final String pluginVersion;

  private LocalProvenanceIdentity(
      String applicationId, String attemptId, String pluginVersion) {
    Provenance validated =
        new Provenance(applicationId, attemptId, pluginVersion, 0L);
    this.applicationId = validated.app();
    this.attemptId = validated.attempt();
    this.pluginVersion = validated.pluginVersion();
  }

  /**
   * Creates caller-redacted framework identity.
   *
   * @param applicationId nonempty strict UTF-8, at most 255 encoded bytes
   * @param attemptId optional strict UTF-8 attempt identity, at most 64 encoded bytes
   * @param pluginVersion nonempty strict UTF-8 producer version, at most 64 encoded bytes
   * @return immutable validated identity
   */
  public static LocalProvenanceIdentity of(
      String applicationId, String attemptId, String pluginVersion) {
    return new LocalProvenanceIdentity(applicationId, attemptId, pluginVersion);
  }

  /**
   * Returns the non-null caller-redacted application identity.
   *
   * @return application identity
   */
  public String applicationId() {
    return applicationId;
  }

  /**
   * Returns the caller-redacted attempt identity, or null when absent.
   *
   * @return nullable attempt identity
   */
  public String attemptId() {
    return attemptId;
  }

  /**
   * Returns the non-null caller-redacted plugin or producer version.
   *
   * @return plugin or producer version
   */
  public String pluginVersion() {
    return pluginVersion;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalProvenanceIdentity)) {
      return false;
    }
    LocalProvenanceIdentity that = (LocalProvenanceIdentity) other;
    return applicationId.equals(that.applicationId) &&
        Objects.equals(attemptId, that.attemptId) &&
        pluginVersion.equals(that.pluginVersion);
  }

  @Override
  public int hashCode() {
    int result = applicationId.hashCode();
    result = 31 * result + Objects.hashCode(attemptId);
    return 31 * result + pluginVersion.hashCode();
  }

  @Override
  public String toString() {
    return "LocalProvenanceIdentity{" +
        "applicationId=<redacted>, attemptIdPresent=" + (attemptId != null) +
        ", pluginVersion=<redacted>}";
  }
}

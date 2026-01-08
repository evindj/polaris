/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.storage.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialsVendor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage subscoped credential cache. */
public class StorageCredentialCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCredentialCache.class);

  private final PolarisDiagnostics diagnostics;
  private final LoadingCache<StorageCredentialCacheKey, StorageCredentialCacheEntry> cache;

  /** Initialize the creds cache */
  public StorageCredentialCache(
      PolarisDiagnostics diagnostics, StorageCredentialCacheConfig cacheConfig) {
    this.diagnostics = diagnostics;
    cache =
        Caffeine.newBuilder()
            .maximumSize(cacheConfig.maxEntries())
            .expireAfter(
                Expiry.creating(
                    (StorageCredentialCacheKey key, StorageCredentialCacheEntry entry) -> {
                      long expireAfterMillis =
                          Math.max(
                              0,
                              Math.min(
                                  (entry.getExpirationTime() - System.currentTimeMillis()) / 2,
                                  entry.getMaxCacheDurationMs()));
                      return Duration.ofMillis(expireAfterMillis);
                    }))
            .build(
                key -> {
                  // the load happen at getOrGenerateSubScopeCreds()
                  return null;
                });
  }

  /** How long credentials should remain in the cache. */
  private long maxCacheDurationMs(RealmConfig realmConfig) {
    var cacheDurationSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS);
    var credentialDurationSeconds =
        realmConfig.getConfig(FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS);
    if (cacheDurationSeconds >= credentialDurationSeconds) {
      throw new IllegalArgumentException(
          String.format(
              "%s should be less than %s",
              FeatureConfiguration.STORAGE_CREDENTIAL_CACHE_DURATION_SECONDS.key(),
              FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS.key()));
    } else {
      return cacheDurationSeconds * 1000L;
    }
  }

  /**
   * Either get from the cache or generate a new entry for a scoped creds
   *
   * @param storageCredentialsVendor the credential vendor used to generate a new scoped creds if
   *     needed
   * @param polarisEntity the polaris entity that is going to scoped creds
   * @param allowListOperation whether allow list action on the provided read and write locations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations.
   * @return the a map of string containing the scoped creds information
   */
  public StorageAccessConfig getOrGenerateSubScopeCreds(
      @Nonnull StorageCredentialsVendor storageCredentialsVendor,
      @Nonnull PolarisEntity polarisEntity,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint) {
    RealmContext realmContext = storageCredentialsVendor.getRealmContext();
    RealmConfig realmConfig = storageCredentialsVendor.getRealmConfig();
    if (!isTypeSupported(polarisEntity.getType())) {
      diagnostics.fail(
          "entity_type_not_suppported_to_scope_creds", "type={}", polarisEntity.getType());
    }

    boolean includePrincipalNameInSubscopedCredential =
        realmConfig.getConfig(FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL);

    StorageCredentialCacheKey key =
        StorageCredentialCacheKey.of(
            realmContext.getRealmIdentifier(),
            polarisEntity,
            allowListOperation,
            allowedReadLocations,
            allowedWriteLocations,
            refreshCredentialsEndpoint,
            includePrincipalNameInSubscopedCredential
                ? Optional.of(polarisPrincipal)
                : Optional.empty());
    LOGGER.atDebug().addKeyValue("key", key).log("subscopedCredsCache");
    Function<StorageCredentialCacheKey, StorageCredentialCacheEntry> loader =
        k -> {
          LOGGER.atDebug().log("StorageCredentialCache::load");
          ScopedCredentialsResult scopedCredentialsResult =
              storageCredentialsVendor.getSubscopedCredsForEntity(
                  polarisEntity,
                  allowListOperation,
                  allowedReadLocations,
                  allowedWriteLocations,
                  polarisPrincipal,
                  refreshCredentialsEndpoint);
          if (scopedCredentialsResult.isSuccess()) {
            long maxCacheDurationMs = maxCacheDurationMs(realmConfig);
            return new StorageCredentialCacheEntry(
                scopedCredentialsResult.getStorageAccessConfig(), maxCacheDurationMs);
          }
          LOGGER
              .atDebug()
              .addKeyValue("errorMessage", scopedCredentialsResult.getExtraInformation())
              .log("Failed to get subscoped credentials");
          throw new UnprocessableEntityException(
              "Failed to get subscoped credentials: %s",
              scopedCredentialsResult.getExtraInformation());
        };
    StorageAccessConfig cachedConfig = cache.get(key, loader).toAccessConfig();

    // Add refreshCredentialsEndpoint to the response if it was provided but not in cache.
    // This allows the same cached credentials to be reused regardless of whether
    // refreshCredentialsEndpoint was specified, avoiding duplicate STS calls.
    return addRefreshEndpointIfNeeded(cachedConfig, refreshCredentialsEndpoint);
  }

  private StorageAccessConfig addRefreshEndpointIfNeeded(
      StorageAccessConfig config, Optional<String> refreshCredentialsEndpoint) {
    if (refreshCredentialsEndpoint.isEmpty()) {
      return config;
    }

    // Check if the refresh endpoint is already present in the extra properties
    String existingEndpoint =
        config
            .extraProperties()
            .get(
                org.apache.polaris.core.storage.StorageAccessProperty
                    .AWS_REFRESH_CREDENTIALS_ENDPOINT
                    .getPropertyName());

    if (existingEndpoint != null && existingEndpoint.equals(refreshCredentialsEndpoint.get())) {
      // Already present with the same value
      return config;
    }

    if (existingEndpoint != null) {
      // Different endpoint requested - log warning and use the requested one
      LOGGER
          .atWarn()
          .addKeyValue("cachedEndpoint", existingEndpoint)
          .addKeyValue("requestedEndpoint", refreshCredentialsEndpoint.get())
          .log("Refresh credentials endpoint mismatch between cache and request");
    }

    // Build new config with the refresh endpoint added
    StorageAccessConfig.Builder builder = StorageAccessConfig.builder();

    // Copy all credentials
    config.credentials().forEach(builder::putCredential);

    // Copy all extra properties
    config.extraProperties().forEach(builder::putExtraProperty);

    // Add the refresh credentials endpoint
    builder.putExtraProperty(
        org.apache.polaris.core.storage.StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT
            .getPropertyName(),
        refreshCredentialsEndpoint.get());

    // Copy all internal properties
    config.internalProperties().forEach(builder::putInternalProperty);

    // Set expiration and credential vending support
    config.expiresAt().ifPresent(builder::expiresAt);
    builder.supportsCredentialVending(config.supportsCredentialVending());

    return builder.build();
  }

  @VisibleForTesting
  @Nullable
  Map<String, String> getIfPresent(StorageCredentialCacheKey key) {
    return getAccessConfig(key).map(StorageAccessConfig::credentials).orElse(null);
  }

  @VisibleForTesting
  Optional<StorageAccessConfig> getAccessConfig(StorageCredentialCacheKey key) {
    return Optional.ofNullable(cache.getIfPresent(key))
        .map(StorageCredentialCacheEntry::toAccessConfig);
  }

  private boolean isTypeSupported(PolarisEntityType type) {
    return type == PolarisEntityType.CATALOG
        || type == PolarisEntityType.NAMESPACE
        || type == PolarisEntityType.TABLE_LIKE
        || type == PolarisEntityType.TASK;
  }

  @VisibleForTesting
  public long getEstimatedSize() {
    return this.cache.estimatedSize();
  }

  @VisibleForTesting
  public void invalidateAll() {
    this.cache.invalidateAll();
  }
}

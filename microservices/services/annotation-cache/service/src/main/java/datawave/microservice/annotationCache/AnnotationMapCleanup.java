package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

/**
 * Periodically destroys empty per-document annotation and fetch maps.
 * <p>
 * All operations that access a document map must use {@link #withDocumentLock(String, Supplier)} so a cleanup cannot destroy the map between obtaining its
 * proxy and completing the operation. For example:
 *
 * <pre>
 * {@code
 * String cacheKey = idType + ":" + documentId;
 * cleanup.withDocumentLock(cacheKey, () -> {
 *     IMap<String,AnnotationMessage> map = hazelcastInstance.getMap(ANNOTATIONS_MAP + cacheKey);
 *     map.put(annotationId, message);
 *     return null;
 * });
 * }
 * </pre>
 *
 * Callers should obtain the map inside the locked operation and must not retain the proxy for later use because destroying a distributed object invalidates
 * existing proxies. Cleanup is disabled by default and should only be enabled after every map access follows this locking protocol.
 */
@Component
public class AnnotationMapCleanup {
    private static final Logger log = LoggerFactory.getLogger(AnnotationMapCleanup.class);

    static final String LIFECYCLE_LOCK_MAP = "annotation-cache-map-lifecycle-locks";
    static final String EMPTY_CANDIDATE_MAP = "annotation-cache-empty-map-candidates";
    private static final String SWEEP_LOCK_KEY = "__empty-map-sweep__";

    private final HazelcastInstance hazelcastInstance;
    private final IMap<String,Boolean> lifecycleLocks;
    private final IMap<String,Long> emptyCandidates;
    private final Duration emptyMapGracePeriod;
    private final Duration cleanupLockWait;
    private final boolean cleanupEnabled;

    public AnnotationMapCleanup(HazelcastInstance hazelcastInstance, AnnotationCacheProperties properties) {
        Duration configuredGracePeriod = properties.getEmptyMapGracePeriod();
        if (configuredGracePeriod == null || configuredGracePeriod.isNegative()) {
            throw new IllegalArgumentException("Empty-map grace period must be non-negative");
        }
        Duration configuredLockWait = properties.getCleanupLockWait();
        if (configuredLockWait == null || configuredLockWait.isNegative()) {
            throw new IllegalArgumentException("Cleanup lock wait must be non-negative");
        }
        Duration configuredCleanupInterval = properties.getMapCleanupInterval();
        if (configuredCleanupInterval == null || configuredCleanupInterval.isZero() || configuredCleanupInterval.isNegative()) {
            throw new IllegalArgumentException("Map cleanup interval must be positive");
        }

        this.hazelcastInstance = hazelcastInstance;
        this.emptyMapGracePeriod = configuredGracePeriod;
        this.cleanupLockWait = configuredLockWait;
        this.cleanupEnabled = properties.isMapCleanupEnabled();
        this.lifecycleLocks = hazelcastInstance.getMap(LIFECYCLE_LOCK_MAP);
        this.emptyCandidates = hazelcastInstance.getMap(EMPTY_CANDIDATE_MAP);
    }

    /** Finds document maps that have remained empty and destroys them. */
    @Scheduled(fixedDelayString = "${annotation-cache.map-cleanup-interval:PT5M}", initialDelayString = "${annotation-cache.map-cleanup-interval:PT5M}")
    public void cleanEmptyMaps() {
        if (!cleanupEnabled) {
            return;
        }

        boolean sweepLockAcquired = false;

        try {
            sweepLockAcquired = lifecycleLocks.tryLock(SWEEP_LOCK_KEY, 0, TimeUnit.MILLISECONDS);
            if (!sweepLockAcquired) {
                return;
            }

            sweepExistingMaps();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while cleaning empty annotation maps", e);
        } catch (HazelcastInstanceNotActiveException e) {
            log.debug("Hazelcast stopped during annotation map cleanup", e);
        } finally {
            if (sweepLockAcquired) {
                unlockSweep();
            }
        }
    }

    /**
     * Executes an operation while preventing cleanup of a document's maps.
     *
     * @param cacheKey
     *            identifier type and document ID
     * @param operation
     *            operation accessing the document maps
     * @return the result of the operation
     * @param <T>
     *            operation result type
     */
    public <T> T withDocumentLock(String cacheKey, Supplier<T> operation) {
        return withDocumentLock(lifecycleLocks, emptyCandidates, cacheKey, operation);
    }

    /**
     * Executes a document-map operation under the shared lifecycle lock without requiring a reference to this Spring component.
     *
     * @param hazelcastInstance
     *            the Hazelcast instance containing the document map
     * @param cacheKey
     *            identifier type and document ID
     * @param operation
     *            operation that obtains and uses the document map
     * @return the result of the operation
     * @param <T>
     *            operation result type
     */
    public static <T> T withDocumentLock(HazelcastInstance hazelcastInstance, String cacheKey, Supplier<T> operation) {
        Objects.requireNonNull(hazelcastInstance, "hazelcastInstance");
        IMap<String,Boolean> lifecycleLocks = hazelcastInstance.getMap(LIFECYCLE_LOCK_MAP);
        IMap<String,Long> emptyCandidates = hazelcastInstance.getMap(EMPTY_CANDIDATE_MAP);
        return withDocumentLock(lifecycleLocks, emptyCandidates, cacheKey, operation);
    }

    private static <T> T withDocumentLock(IMap<String,Boolean> lifecycleLocks, IMap<String,Long> emptyCandidates, String cacheKey, Supplier<T> operation) {
        Objects.requireNonNull(cacheKey, "cacheKey");
        Objects.requireNonNull(operation, "operation");

        lifecycleLocks.lock(cacheKey);
        try {
            emptyCandidates.remove(ANNOTATIONS_MAP + cacheKey);
            emptyCandidates.remove(FETCH_MAP + cacheKey);
            return operation.get();
        } finally {
            lifecycleLocks.unlock(cacheKey);
        }
    }

    private void sweepExistingMaps() throws InterruptedException {
        Set<String> existingMapNames = new HashSet<>();
        long now = System.currentTimeMillis();

        for (DistributedObject object : hazelcastInstance.getDistributedObjects()) {
            if (!(object instanceof IMap)) {
                continue;
            }

            String mapName = object.getName();
            if (!isDocumentMap(mapName)) {
                continue;
            }

            existingMapNames.add(mapName);
            inspectMap((IMap<?,?>) object, now);
        }

        removeStaleCandidates(existingMapNames);
    }

    private void inspectMap(IMap<?,?> map, long now) throws InterruptedException {
        String mapName = map.getName();

        try {
            if (!map.isEmpty()) {
                emptyCandidates.remove(mapName);
                return;
            }

            Long emptySince = emptyCandidates.putIfAbsent(mapName, now);
            if (emptySince == null || now - emptySince < emptyMapGracePeriod.toMillis()) {
                return;
            }

            destroyIfStillEmpty(map);
        } catch (DistributedObjectDestroyedException e) {
            emptyCandidates.remove(mapName);
        } catch (HazelcastInstanceNotActiveException e) {
            throw e;
        } catch (RuntimeException e) {
            log.warn("Unable to inspect map {} for cleanup", mapName, e);
        }
    }

    private void destroyIfStillEmpty(IMap<?,?> map) throws InterruptedException {
        String mapName = map.getName();
        String cacheKey = cacheKey(mapName);
        boolean lockAcquired = lifecycleLocks.tryLock(cacheKey, cleanupLockWait.toMillis(), TimeUnit.MILLISECONDS);

        if (!lockAcquired) {
            return;
        }

        try {
            if (!map.isEmpty()) {
                emptyCandidates.remove(mapName);
                return;
            }

            if (isWriteBehind(mapName)) {
                map.flush();
            }

            if (!map.isEmpty()) {
                emptyCandidates.remove(mapName);
                return;
            }

            map.destroy();
            emptyCandidates.remove(mapName);
            log.info("Destroyed empty Hazelcast map {}", mapName);
        } finally {
            lifecycleLocks.unlock(cacheKey);
        }
    }

    private boolean isWriteBehind(String mapName) {
        MapStoreConfig storeConfig = hazelcastInstance.getConfig().findMapConfig(mapName).getMapStoreConfig();
        return storeConfig != null && storeConfig.isEnabled() && storeConfig.getWriteDelaySeconds() > 0;
    }

    private boolean isDocumentMap(String mapName) {
        return hasCacheKey(mapName, ANNOTATIONS_MAP) || hasCacheKey(mapName, FETCH_MAP);
    }

    private boolean hasCacheKey(String mapName, String prefix) {
        return mapName.startsWith(prefix) && mapName.length() > prefix.length();
    }

    private String cacheKey(String mapName) {
        if (hasCacheKey(mapName, ANNOTATIONS_MAP)) {
            return mapName.substring(ANNOTATIONS_MAP.length());
        }
        if (hasCacheKey(mapName, FETCH_MAP)) {
            return mapName.substring(FETCH_MAP.length());
        }
        throw new IllegalArgumentException("Not a document map: " + mapName);
    }

    private void removeStaleCandidates(Set<String> existingMapNames) {
        for (String mapName : new HashSet<>(emptyCandidates.keySet())) {
            if (!existingMapNames.contains(mapName)) {
                emptyCandidates.remove(mapName);
            }
        }
    }

    private void unlockSweep() {
        try {
            lifecycleLocks.unlock(SWEEP_LOCK_KEY);
        } catch (HazelcastInstanceNotActiveException | DistributedObjectDestroyedException e) {
            log.debug("Hazelcast stopped while releasing cleanup lock", e);
        }
    }
}

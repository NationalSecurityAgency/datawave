package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.ReplicaMigrationEvent;

import datawave.microservice.annotationCache.api.entryProcessor.UnionAnnotationIdsProcessor;
import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

/**
 * Periodically repairs missing entries in the derived document annotation index.
 *
 * <p>
 * The annotation maps are the source of truth. The document index is derived state and may be rebuilt after listener failures, member loss, partition
 * migration, or a merge. This component deliberately performs add-only repair; removal remains the responsibility of the annotation entry listener because
 * deleting IDs from a snapshot can race with expiration or a new write.
 * </p>
 *
 * <p>
 * The annotation maps are authoritative. Reconciliation is deliberately add-only so a scan racing with a write or expiration cannot remove a valid index entry.
 * Stale IDs are measured and logged, but normal entry listeners remain responsible for removing them.
 * </p>
 */
@Component
public class AnnotationCacheReconciler implements MembershipListener, LifecycleListener, MigrationListener, PartitionLostListener, DistributedObjectListener {
    static final String CONTROL_MAP = "annotation-cache-reconciliation-control";
    private static final String RUN_LOCK = "run-lock";
    private static final String REQUESTED_AT = "requested-at";
    private static final String INVALIDATE_FETCH = "invalidate-fetch";
    private static final String LAST_COMPLETED_AT = "last-completed-at";
    private static final String CURSOR = "cursor";
    private static final Logger log = LoggerFactory.getLogger(AnnotationCacheReconciler.class);

    private final HazelcastInstance hazelcastInstance;
    private final AnnotationCacheProperties properties;
    private final Set<String> mapNames = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final AtomicLong localRequest = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean localFetchInvalidation = new AtomicBoolean(false);

    private final UUID membershipListenerId;
    private final UUID lifecycleListenerId;
    private final UUID migrationListenerId;
    private final UUID partitionLostListenerId;
    private final UUID distributedObjectListenerId;

    /**
     * Registers topology listeners before discovering existing maps, then schedules the first reconciliation request. Registering the distributed-object
     * listener first closes the map discovery race where a map could be created between the initial snapshot and registration.
     *
     * @param hazelcastInstance
     *            the shared Hazelcast cluster
     * @param properties
     *            reconciliation configuration
     */
    public AnnotationCacheReconciler(HazelcastInstance hazelcastInstance, AnnotationCacheProperties properties) {
        this.hazelcastInstance = hazelcastInstance;
        this.properties = properties;
        validateProperties();

        distributedObjectListenerId = hazelcastInstance.addDistributedObjectListener(this);
        // Register before taking the initial snapshot so a map created concurrently cannot fall between discovery and listener registration.
        for (DistributedObject object : hazelcastInstance.getDistributedObjects()) {
            track(object);
        }
        membershipListenerId = hazelcastInstance.getCluster().addMembershipListener(this);
        lifecycleListenerId = hazelcastInstance.getLifecycleService().addLifecycleListener(this);
        migrationListenerId = hazelcastInstance.getPartitionService().addMigrationListener(this);
        partitionLostListenerId = hazelcastInstance.getPartitionService().addPartitionLostListener(this);
    }

    /** Validates settings that would otherwise cause a disabled or continuously failing scheduler. */
    private void validateProperties() {
        if (properties.getReconciliationInterval() == null || properties.getReconciliationInterval().isZero()
                        || properties.getReconciliationInterval().isNegative()) {
            throw new IllegalStateException("annotation-cache.reconciliation-interval must be positive");
        }
        if (properties.getReconciliationSettleDelay() == null || properties.getReconciliationSettleDelay().isNegative()) {
            throw new IllegalStateException("annotation-cache.reconciliation-settle-delay must be non-negative");
        }
        if (properties.getReconciliationMaxMapsPerRun() <= 0) {
            throw new IllegalStateException("annotation-cache.reconciliation-max-maps-per-run must be positive");
        }
    }

    /**
     * Polls for periodic or topology-triggered work. The distributed lock ensures only one member performs a pass.
     *
     * <p>
     * The sequence is intentionally ordered: publish local requests, wait for the debounce interval, wait for partition safety, acquire the cluster-wide run
     * lock, and only then scan. This keeps event callbacks lightweight and avoids scanning while Hazelcast is moving data.
     * </p>
     */
    @Scheduled(fixedDelayString = "${annotation-cache.reconciliation-poll-interval-ms:5000}",
                    initialDelayString = "${annotation-cache.reconciliation-poll-interval-ms:5000}")
    public void poll() {
        if (!properties.isReconciliationEnabled() || !hazelcastInstance.getLifecycleService().isRunning()) {
            return;
        }

        // Each member records local events, but all members publish into one shared control map.
        IMap<String,Long> control = hazelcastInstance.getMap(CONTROL_MAP);
        publishLocalRequest(control);

        // Event-triggered work is debounced so a rolling restart or rebalance produces one pass,
        // while the periodic deadline remains a fallback for missed listener events.
        long now = System.currentTimeMillis();
        Long requestedAt = control.get(REQUESTED_AT);
        Long lastCompletedAt = control.get(LAST_COMPLETED_AT);
        boolean eventDue = requestedAt != null && now - requestedAt >= properties.getReconciliationSettleDelay().toMillis();
        boolean periodicDue = lastCompletedAt == null || now - lastCompletedAt >= properties.getReconciliationInterval().toMillis();
        if (!eventDue && !periodicDue) {
            return;
        }
        // Partition safety means migrations have settled enough for the scan to observe a useful
        // snapshot. The scan itself is still eventually consistent with concurrent writes/expiry.
        if (!hazelcastInstance.getPartitionService().isClusterSafe()) {
            log.debug("Deferring annotation cache reconciliation until the Hazelcast cluster is safe");
            return;
        }
        // A normal IMap lock is sufficient here because this is a best-effort singleton worker;
        // post-merge and periodic passes provide recovery if two sides briefly run independently.
        if (!control.tryLock(RUN_LOCK)) {
            return;
        }

        try {
            // Re-read state after acquiring the lock. Another member may have completed the work
            // while this member was waiting for the lock.
            now = System.currentTimeMillis();
            requestedAt = control.get(REQUESTED_AT);
            lastCompletedAt = control.get(LAST_COMPLETED_AT);
            eventDue = requestedAt != null && now - requestedAt >= properties.getReconciliationSettleDelay().toMillis();
            periodicDue = lastCompletedAt == null || now - lastCompletedAt >= properties.getReconciliationInterval().toMillis();
            if (!eventDue && !periodicDue) {
                return;
            }

            // Merge/partition events invalidate fetch metadata before repairing the index. This
            // guarantees a later read will not trust freshness state from an inconsistent topology.
            Long invalidationRequest = control.get(INVALIDATE_FETCH);
            if (invalidationRequest != null) {
                clearAllFetchMaps();
                control.remove(INVALIDATE_FETCH, invalidationRequest);
            }

            // Process only a bounded batch so reconciliation cannot monopolize Hazelcast threads.
            ReconciliationResult result = reconcileBatch(control);
            if (result.failures == 0 && result.cycleComplete) {
                // A complete successful cycle establishes the next periodic deadline. Failures do
                // not advance it, allowing the next poll to retry the failed maps.
                control.set(LAST_COMPLETED_AT, System.currentTimeMillis());
                if (requestedAt != null) {
                    control.remove(REQUESTED_AT, requestedAt);
                }
            } else if (result.failures == 0) {
                // Continue a bounded scan on the next poll instead of waiting for the periodic interval.
                setIfLater(control, REQUESTED_AT, System.currentTimeMillis() - properties.getReconciliationSettleDelay().toMillis());
            }

            log.info("Annotation cache reconciliation scanned {} maps, repaired {} missing IDs, observed {} stale IDs, and had {} failures{}",
                            result.mapsScanned, result.missingIdsRepaired, result.staleIdsObserved, result.failures,
                            result.cycleComplete ? "" : "; more maps remain");
        } catch (HazelcastInstanceNotActiveException e) {
            log.debug("Hazelcast stopped during annotation cache reconciliation", e);
        } catch (RuntimeException e) {
            log.error("Annotation cache reconciliation failed", e);
        } finally {
            try {
                control.unlock(RUN_LOCK);
            } catch (HazelcastInstanceNotActiveException e) {
                log.debug("Hazelcast stopped before the annotation reconciliation lock could be released");
            }
        }
    }

    /**
     * Publishes requests recorded by this member into the shared control map. Conditional updates preserve the oldest request time, preventing a later event
     * from postponing an already due reconciliation indefinitely.
     */
    private void publishLocalRequest(IMap<String,Long> control) {
        long requestedAt = localRequest.getAndSet(0);
        if (requestedAt != 0) {
            setIfLater(control, REQUESTED_AT, requestedAt);
        }
        if (localFetchInvalidation.getAndSet(false)) {
            setIfLater(control, INVALIDATE_FETCH, System.currentTimeMillis());
        }
    }

    /** Atomically stores the later request timestamp without overwriting a newer request. */
    private void setIfLater(IMap<String,Long> control, String key, long timestamp) {
        while (true) {
            Long existing = control.get(key);
            if (existing != null && existing >= timestamp) {
                return;
            }
            if (existing == null) {
                if (control.putIfAbsent(key, timestamp) == null) {
                    return;
                }
            } else if (control.replace(key, existing, timestamp)) {
                return;
            }
        }
    }

    /**
     * Scans one bounded portion of the known annotation maps.
     *
     * <p>
     * The cursor is stored in Hazelcast so any member can continue work after the current reconciler stops. Each map is read independently; a failure in one
     * map does not prevent the remaining maps in the batch from being attempted.
     * </p>
     *
     * @param control
     *            shared reconciliation state containing the scan cursor
     * @return counts and whether the current cursor completed a full cycle
     */
    ReconciliationResult reconcileBatch(IMap<String,Long> control) {
        // Sort names so the persisted cursor has deterministic behavior as members take turns.
        List<String> annotationsMaps = mapNames.stream().filter(name -> name.startsWith(ANNOTATIONS_MAP) && name.length() > ANNOTATIONS_MAP.length())
                        .sorted(Comparator.naturalOrder()).collect(java.util.stream.Collectors.toList());
        if (annotationsMaps.isEmpty()) {
            control.set(CURSOR, 0L);
            return new ReconciliationResult(0, 0, 0, 0, true);
        }

        int start = Math.floorMod(control.getOrDefault(CURSOR, 0L).intValue(), annotationsMaps.size());
        int count = Math.min(properties.getReconciliationMaxMapsPerRun(), annotationsMaps.size() - start);
        int repaired = 0;
        int stale = 0;
        int failures = 0;
        IMap<String,Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);

        for (int offset = 0; offset < count; offset++) {
            String mapName = annotationsMaps.get(start + offset);
            String cacheKey = mapName.substring(ANNOTATIONS_MAP.length());
            try {
                IMap<String,Object> annotationMap = hazelcastInstance.getMap(mapName);
                // Read the actual keys first, then compare them with the derived index. We only union
                // missing IDs: removing IDs from this non-atomic snapshot could delete a concurrently
                // written annotation from the index.
                Set<String> actualIds = annotationMap.keySet();
                Set<String> indexedIds = documentIndex.get(cacheKey);
                Set<String> missingIds = new HashSet<>(actualIds);
                if (indexedIds != null) {
                    missingIds.removeAll(indexedIds);
                    Set<String> staleIds = new HashSet<>(indexedIds);
                    staleIds.removeAll(actualIds);
                    stale += staleIds.size();
                    if (!staleIds.isEmpty()) {
                        log.debug("Document index {} contains {} stale annotation IDs", cacheKey, staleIds.size());
                    }
                }

                if (!missingIds.isEmpty()) {
                    // EntryProcessor makes the set union atomic at the index key and remains safe
                    // when listeners or another reconciliation member perform the same repair.
                    documentIndex.executeOnKey(cacheKey, new UnionAnnotationIdsProcessor(missingIds));
                    hazelcastInstance.getMap(FETCH_MAP + cacheKey).clear();
                    repaired += missingIds.size();
                    log.warn("Repaired {} missing annotation IDs in document index {}", missingIds.size(), cacheKey);
                }
            } catch (RuntimeException e) {
                failures++;
                log.error("Failed to reconcile annotation map {}", mapName, e);
            }
        }

        int next = start + count == annotationsMaps.size() ? 0 : start + count;
        boolean cycleComplete = next == 0;
        control.set(CURSOR, (long) next);
        return new ReconciliationResult(count, repaired, stale, failures, cycleComplete);
    }

    /**
     * Invalidates all fetch metadata after a merge or partition loss. Fetch records are not authoritative, so discarding them is safer than allowing stale
     * metadata to suppress a permanent-storage lookup.
     */
    private void clearAllFetchMaps() {
        int cleared = 0;
        for (String mapName : new ArrayList<>(mapNames)) {
            if (mapName.startsWith(FETCH_MAP) && mapName.length() > FETCH_MAP.length()) {
                hazelcastInstance.getMap(mapName).clear();
                cleared++;
            }
        }
        log.warn("Cleared {} annotation fetch maps after a Hazelcast partition or merge event", cleared);
    }

    /** Records a debounced topology-triggered reconciliation request for the next safe poll. */
    private void requestReconciliation(String reason, boolean invalidateFetch) {
        long now = System.currentTimeMillis();
        localRequest.set(now);
        if (invalidateFetch) {
            localFetchInvalidation.set(true);
        }
        log.info("Requested annotation cache reconciliation after {}", reason);
    }

    /** A new member causes partition redistribution and may expose pre-existing index gaps. */
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        requestReconciliation("Hazelcast member addition", false);
    }

    /** A removed member may have failed between updating an annotation and its derived index. */
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        requestReconciliation("Hazelcast member removal", false);
    }

    /** Requests repair after split-brain merge activity, when independently merged maps may differ. */
    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
            requestReconciliation("Hazelcast split-brain merge", true);
        } else if (event.getState() == LifecycleEvent.LifecycleState.MERGE_FAILED) {
            requestReconciliation("Hazelcast split-brain merge failure", true);
        }
    }

    /** Migration start is intentionally ignored; reconciliation waits for cluster safety in poll(). */
    @Override
    public void migrationStarted(MigrationState migrationState) {}

    /** Migration completion is intentionally ignored; one completion event is not a full-cycle signal. */
    @Override
    public void migrationFinished(MigrationState migrationState) {}

    /** Successful replica migration does not require immediate work. */
    @Override
    public void replicaMigrationCompleted(ReplicaMigrationEvent event) {}

    /** A failed replica migration requests repair and records an operationally significant error. */
    @Override
    public void replicaMigrationFailed(ReplicaMigrationEvent event) {
        log.error("Hazelcast replica migration failed for partition {}", event.getPartitionId());
        requestReconciliation("Hazelcast replica migration failure", false);
    }

    /**
     * Partition loss invalidates fetch metadata globally because the affected document maps cannot be identified cheaply from the partition event.
     * Reconciliation can repair surviving entries, while Sonicweb can reload missing content from permanent storage.
     */
    @Override
    public void partitionLost(PartitionLostEvent event) {
        log.error("Hazelcast partition {} lost {} backups (all replicas lost: {})", event.getPartitionId(), event.getLostBackupCount(),
                        event.allReplicasInPartitionLost());
        requestReconciliation("Hazelcast partition loss", true);
    }

    /** Tracks newly created annotation and fetch maps for future scans and invalidation. */
    @Override
    public void distributedObjectCreated(DistributedObjectEvent event) {
        track(event.getDistributedObject());
    }

    /** Removes destroyed maps so later scans do not recreate obsolete distributed objects. */
    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        mapNames.remove(String.valueOf(event.getObjectName()));
    }

    /** Adds only maps relevant to reconciliation; the control/index maps are accessed directly. */
    private void track(DistributedObject object) {
        if (object instanceof IMap) {
            String name = object.getName();
            if (name.startsWith(ANNOTATIONS_MAP) || name.startsWith(FETCH_MAP)) {
                mapNames.add(name);
            }
        }
    }

    /** Unregisters listeners during shutdown to prevent callbacks against a stopped Hazelcast instance. */
    @PreDestroy
    public void close() {
        if (!hazelcastInstance.getLifecycleService().isRunning()) {
            return;
        }
        hazelcastInstance.getCluster().removeMembershipListener(membershipListenerId);
        hazelcastInstance.getLifecycleService().removeLifecycleListener(lifecycleListenerId);
        hazelcastInstance.getPartitionService().removeMigrationListener(migrationListenerId);
        hazelcastInstance.getPartitionService().removePartitionLostListener(partitionLostListenerId);
        hazelcastInstance.removeDistributedObjectListener(distributedObjectListenerId);
    }

    static class ReconciliationResult {
        final int mapsScanned;
        final int missingIdsRepaired;
        final int staleIdsObserved;
        final int failures;
        final boolean cycleComplete;

        ReconciliationResult(int mapsScanned, int missingIdsRepaired, int staleIdsObserved, int failures, boolean cycleComplete) {
            this.mapsScanned = mapsScanned;
            this.missingIdsRepaired = missingIdsRepaired;
            this.staleIdsObserved = staleIdsObserved;
            this.failures = failures;
            this.cycleComplete = cycleComplete;
        }
    }
}

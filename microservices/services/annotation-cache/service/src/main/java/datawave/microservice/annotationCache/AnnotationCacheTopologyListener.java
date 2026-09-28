package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

/** Invalidates transient fetch records after topology events that may have changed cached annotation availability. */
@Component
public class AnnotationCacheTopologyListener
                implements MembershipListener, LifecycleListener, MigrationListener, PartitionLostListener, DistributedObjectListener {
    private static final Logger log = LoggerFactory.getLogger(AnnotationCacheTopologyListener.class);

    private final HazelcastInstance hazelcastInstance;
    private final AnnotationCacheProperties properties;
    private final Set<String> fetchMapNames = ConcurrentHashMap.newKeySet();
    private final AtomicLong invalidationRequestedAt = new AtomicLong();

    private final UUID membershipListenerId;
    private final UUID lifecycleListenerId;
    private final UUID migrationListenerId;
    private final UUID partitionLostListenerId;
    private final UUID distributedObjectListenerId;

    public AnnotationCacheTopologyListener(HazelcastInstance hazelcastInstance, AnnotationCacheProperties properties) {
        this.hazelcastInstance = hazelcastInstance;
        this.properties = properties;
        validateProperties();

        // Subscribe before taking the initial snapshot so a fetch map cannot be missed during discovery.
        distributedObjectListenerId = hazelcastInstance.addDistributedObjectListener(this);
        for (DistributedObject object : hazelcastInstance.getDistributedObjects()) {
            track(object);
        }
        membershipListenerId = hazelcastInstance.getCluster().addMembershipListener(this);
        lifecycleListenerId = hazelcastInstance.getLifecycleService().addLifecycleListener(this);
        migrationListenerId = hazelcastInstance.getPartitionService().addMigrationListener(this);
        partitionLostListenerId = hazelcastInstance.getPartitionService().addPartitionLostListener(this);
    }

    private void validateProperties() {
        if (properties.getTopologySettleDelay() == null || properties.getTopologySettleDelay().isNegative()) {
            throw new IllegalStateException("annotation-cache.topology-settle-delay must be non-negative");
        }
        if (properties.getTopologyPollIntervalMs() <= 0) {
            throw new IllegalStateException("annotation-cache.topology-poll-interval-ms must be positive");
        }
    }

    /** Polls after the topology settles; clearing fetch maps is idempotent and safe for every cluster member to perform. */
    @Scheduled(fixedDelayString = "${annotation-cache.topology-poll-interval-ms:5000}",
                    initialDelayString = "${annotation-cache.topology-poll-interval-ms:5000}")
    public void poll() {
        if (!properties.isTopologyMonitoringEnabled() || !hazelcastInstance.getLifecycleService().isRunning()) {
            return;
        }

        long requestedAt = invalidationRequestedAt.get();
        if (requestedAt == 0 || System.currentTimeMillis() - requestedAt < properties.getTopologySettleDelay().toMillis()
                        || !hazelcastInstance.getPartitionService().isClusterSafe()) {
            return;
        }

        try {
            int cleared = 0;
            for (String mapName : Set.copyOf(fetchMapNames)) {
                hazelcastInstance.getMap(mapName).clear();
                cleared++;
            }
            if (invalidationRequestedAt.compareAndSet(requestedAt, 0)) {
                log.warn("Cleared {} annotation fetch maps after a Hazelcast topology event", cleared);
            }
        } catch (HazelcastInstanceNotActiveException e) {
            log.debug("Hazelcast stopped during topology-triggered fetch invalidation", e);
        } catch (RuntimeException e) {
            log.error("Failed to invalidate annotation fetch maps after a Hazelcast topology event", e);
        }
    }

    private void requestInvalidation(String reason) {
        invalidationRequestedAt.set(System.currentTimeMillis());
        log.info("Requested annotation fetch invalidation after {}", reason);
    }

    @Override
    public void memberAdded(MembershipEvent event) {
        // Adding a member redistributes data but does not by itself imply data loss.
    }

    @Override
    public void memberRemoved(MembershipEvent event) {
        requestInvalidation("Hazelcast member removal");
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState() == LifecycleEvent.LifecycleState.MERGED || event.getState() == LifecycleEvent.LifecycleState.MERGE_FAILED) {
            requestInvalidation("Hazelcast split-brain merge event " + event.getState());
        }
    }

    @Override
    public void migrationStarted(MigrationState migrationState) {}

    @Override
    public void migrationFinished(MigrationState migrationState) {}

    @Override
    public void replicaMigrationCompleted(ReplicaMigrationEvent event) {}

    @Override
    public void replicaMigrationFailed(ReplicaMigrationEvent event) {
        log.error("Hazelcast replica migration failed for partition {}", event.getPartitionId());
        requestInvalidation("Hazelcast replica migration failure");
    }

    @Override
    public void partitionLost(PartitionLostEvent event) {
        log.error("Hazelcast partition {} lost {} backups (all replicas lost: {})", event.getPartitionId(), event.getLostBackupCount(),
                        event.allReplicasInPartitionLost());
        requestInvalidation("Hazelcast partition loss");
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent event) {
        track(event.getDistributedObject());
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        fetchMapNames.remove(String.valueOf(event.getObjectName()));
    }

    private void track(DistributedObject object) {
        if (object instanceof IMap) {
            String name = object.getName();
            if (name.startsWith(FETCH_MAP) && name.length() > FETCH_MAP.length()) {
                fetchMapNames.add(name);
            }
        }
    }

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
}

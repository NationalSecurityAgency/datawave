package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.RegionConfiguration;
import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

/**
 * Consumes federated annotations and transiently caches them locally. Since they were already published in their source region, transient insertion avoids
 * republishing them and creating a federation loop.
 */
@Configuration
public class LoadCacheConsumer {
    private static final Logger log = LoggerFactory.getLogger(LoadCacheConsumer.class);
    private final HazelcastInstance hazelcastInstance;
    private final String localRegion;
    private final Duration federationLockWait;

    public LoadCacheConsumer(HazelcastInstance hazelcastInstance, RegionConfiguration regionConfiguration, AnnotationCacheProperties properties) {
        this.hazelcastInstance = hazelcastInstance;
        if (regionConfiguration == null || regionConfiguration.getName() == null || regionConfiguration.getName().isBlank()) {
            throw new IllegalStateException("region.name must be configured for the federated annotation consumer");
        }
        if (properties == null || properties.getFederationLockWait() == null || properties.getFederationLockWait().isZero()
                        || properties.getFederationLockWait().isNegative() || properties.getFederationLockWait().toMillis() <= 0) {
            throw new IllegalStateException("annotation-cache.federation-lock-wait must be positive");
        }
        this.localRegion = regionConfiguration.getName();
        this.federationLockWait = properties.getFederationLockWait();
        log.info("LoadCacheConsumer activated for region {} with Hazelcast: {} and federation lock wait {}", localRegion, hazelcastInstance,
                        federationLockWait);
    }

    /**
     * Creates the message consumer. It ignores messages from this region, validates the origin and identifier-type metadata on federated messages, then
     * processes each annotation individually.
     *
     * @return consumer for annotation messages from RabbitMQ
     */
    @Bean
    public Consumer<AnnotationMessage> loadCache() {
        return annotationMessage -> {
            String sourceRegion = annotationMessage.getParametersOrDefault(REGION_ID_PARAMETER, "");
            if (sourceRegion.isBlank()) {
                throw new IllegalArgumentException("Annotation message is missing source region parameter " + REGION_ID_PARAMETER);
            }
            if (localRegion.equals(sourceRegion)) {
                log.debug("Ignoring annotation message generated in local region {}", localRegion);
                return;
            }

            String idType = annotationMessage.getParametersOrDefault(ID_TYPE_PARAMETER, "");
            if (idType.isBlank()) {
                throw new IllegalArgumentException("Federated annotation message is missing parameter " + ID_TYPE_PARAMETER);
            }

            for (Annotation annotation : annotationMessage.getAnnotationsList()) {
                addToCache(idType, annotationMessage, annotation);
            }
        };
    }

    /**
     * Caches one annotation from a federated message in the shared annotation map. A distributed lock makes the check-and-insert idempotent across members;
     * transient insertion avoids republishing the message through the local MapStore while retaining the map's configured expiration.
     */
    private void addToCache(String idType, AnnotationMessage annotationMessage, Annotation annotation) {
        String documentId = annotation.getDocumentId();
        String annotationId = annotation.getAnnotationId();
        if (documentId.isBlank()) {
            throw new IllegalArgumentException("Federated annotation " + annotationId + " is missing its document ID");
        }
        if (annotationId.isBlank()) {
            throw new IllegalArgumentException("Federated annotation for document " + documentId + " is missing its annotation ID");
        }

        AnnotationKey key = new AnnotationKey(idType, documentId, annotationId);
        IMap<AnnotationKey,AnnotationMessage> annotationMap = hazelcastInstance.getMap(ANNOTATIONS_MAP);

        // The normal producer writes one annotation per message. Normalize a batched message so that every map key still contains exactly its annotation.
        AnnotationMessage cacheValue = annotationMessage.getAnnotationsCount() == 1 ? annotationMessage
                        : annotationMessage.toBuilder().clearAnnotations().addAnnotations(annotation).build();

        // Federated messages have already passed through a MapStore in their originating region. A transient put prevents the local MapStore from publishing
        // the message again while retaining the TTL and max-idle behavior configured for the annotation map.
        // Serialize deliveries for this annotation across cluster members: without the lock, duplicates can both pass containsKey and race to insert.
        boolean lockAcquired = false;
        try {
            lockAcquired = annotationMap.tryLock(key, federationLockWait.toMillis(), TimeUnit.MILLISECONDS);
            if (!lockAcquired) {
                throw new AnnotationStorageException("Timed out acquiring Hazelcast lock for federated annotation " + key);
            }

            // While holding the lock, ignore duplicate deliveries or insert this annotation without invoking the MapStore.
            if (!annotationMap.containsKey(key)) {
                MapConfig mapConfig = hazelcastInstance.getConfig().findMapConfig(ANNOTATIONS_MAP);
                annotationMap.putTransient(key, cacheValue, mapConfig.getTimeToLiveSeconds(), TimeUnit.SECONDS, mapConfig.getMaxIdleSeconds(),
                                TimeUnit.SECONDS);
                log.info("Stored annotation {} federated from region {} in local cache {}", key, annotationMessage.getParametersOrThrow(REGION_ID_PARAMETER),
                                ANNOTATIONS_MAP);
            } else {
                log.debug("Annotation {} already exists in local cache {}", key, ANNOTATIONS_MAP);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AnnotationStorageException("Interrupted acquiring Hazelcast lock for federated annotation " + key, e);
        } finally {
            // Release only if acquired; otherwise another member's lock must remain untouched.
            if (lockAcquired) {
                annotationMap.unlock(key);
            }
        }

    }
}

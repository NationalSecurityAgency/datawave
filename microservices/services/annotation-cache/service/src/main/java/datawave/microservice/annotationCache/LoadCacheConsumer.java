package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;

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
import datawave.microservice.annotationCache.api.RegionConfiguration;
import datawave.microservice.annotationCache.api.entryProcessor.AppendAnnotationIdProcessor;

/** Consumes federated annotation messages and makes them available in the local Hazelcast cache. */
@Configuration
public class LoadCacheConsumer {
    private static final Logger log = LoggerFactory.getLogger(LoadCacheConsumer.class);

    private final HazelcastInstance hazelcastInstance;
    private final String localRegion;

    public LoadCacheConsumer(HazelcastInstance hazelcastInstance, RegionConfiguration regionConfiguration) {
        this.hazelcastInstance = hazelcastInstance;
        if (regionConfiguration == null || regionConfiguration.getName() == null || regionConfiguration.getName().isBlank()) {
            throw new IllegalStateException("region.name must be configured for the federated annotation consumer");
        }
        this.localRegion = regionConfiguration.getName();
        log.info("LoadCacheConsumer activated for region {} with Hazelcast: {}", localRegion, hazelcastInstance);
    }

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

    private void addToCache(String idType, AnnotationMessage annotationMessage, Annotation annotation) {
        String documentId = annotation.getDocumentId();
        String annotationId = annotation.getAnnotationId();
        if (documentId.isBlank()) {
            throw new IllegalArgumentException("Federated annotation " + annotationId + " is missing its document ID");
        }
        if (annotationId.isBlank()) {
            throw new IllegalArgumentException("Federated annotation for document " + documentId + " is missing its annotation ID");
        }

        String mapName = ANNOTATIONS_MAP + idType + ":" + documentId;
        IMap<String,AnnotationMessage> annotationMap = hazelcastInstance.getMap(mapName);
        IMap<String,java.util.Set<String>> documentIndex = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);

        // The normal producer writes one annotation per message. Normalize a batched message so that every map key still contains exactly its annotation.
        AnnotationMessage cacheValue = annotationMessage.getAnnotationsCount() == 1 ? annotationMessage
                        : annotationMessage.toBuilder().clearAnnotations().addAnnotations(annotation).build();

        // Federated messages have already passed through a MapStore in their originating region. A transient put prevents the local MapStore from publishing
        // the message again while retaining the TTL and max-idle behavior configured for the annotation map.
        annotationMap.lock(annotationId);
        try {
            if (!annotationMap.containsKey(annotationId)) {
                MapConfig mapConfig = hazelcastInstance.getConfig().findMapConfig(mapName);
                annotationMap.putTransient(annotationId, cacheValue, mapConfig.getTimeToLiveSeconds(), TimeUnit.SECONDS, mapConfig.getMaxIdleSeconds(),
                                TimeUnit.SECONDS);
                log.info("Stored annotation {} federated from region {} in local cache {}", annotationId,
                                annotationMessage.getParametersOrThrow(REGION_ID_PARAMETER), mapName);
            } else {
                log.debug("Annotation {} already exists in local cache {}", annotationId, mapName);
            }
        } finally {
            annotationMap.unlock(annotationId);
        }

        // Keep the derived index update idempotent and perform it even when the annotation already existed. This repairs an index missed by a previous event.
        documentIndex.executeOnKey(idType + ":" + documentId, new AppendAnnotationIdProcessor(annotationId));
    }
}

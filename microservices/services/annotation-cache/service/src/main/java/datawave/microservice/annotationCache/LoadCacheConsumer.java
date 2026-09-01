package datawave.microservice.annotationCache;

import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.beust.jcommander.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.microservice.annotationCache.api.AnnotationMessageProto.AnnotationMessage;

/**
 * This is used by application.yml to subscribe to loadCache for incoming messages. They should add annotations to the cache if not already present
 */
@Configuration
public class LoadCacheConsumer {
    private static Logger log = LoggerFactory.getLogger(LoadCacheConsumer.class);

    private final HazelcastInstance hazelcastInstance;

    public LoadCacheConsumer(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        log.info("LoadCacheConsumer successfully created and activated with Hazelcast: " + hazelcastInstance);
    }

    @Bean
    public Consumer<AnnotationMessage> loadCache() {
        return annotationMessage -> {
            Annotation annotation = null;
            try {
                annotation = Annotation.parseFrom(annotationMessage.getAnnotationBytes());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            log.info("got annotation off queue: " + annotation.getAnnotationId());
            if (hazelcastInstance != null) {
                IMap<String,Annotation> annotationMap = hazelcastInstance.getMap("annotations");

                String docId = docKey(annotation);
                if (annotationMap.putIfAbsent(docId, annotation).equals(annotation)) {
                    log.info("stored object from queue to cache: " + annotation.getAnnotationId());
                } else {
                    log.info("existing object already in cache: " + annotation.getAnnotationId());
                }
            }
        };
    }

    private String docKey(Annotation a) {
        return Strings.join("/", List.of(a.getShard(), a.getDataType(), a.getUid(), a.getAnnotationId()));
    }
}

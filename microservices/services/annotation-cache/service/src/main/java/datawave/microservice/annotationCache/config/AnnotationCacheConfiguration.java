package datawave.microservice.annotationCache.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/**
 * Binding configuration to hazelcast
 */
@Configuration
public class AnnotationCacheConfiguration {
    private static Logger log = LoggerFactory.getLogger(AnnotationCacheConfiguration.class);

    @Bean
    public HazelcastInstance hazelcastInstance(Config config, AnnotationMapStore annotationMapStore, AnnotationSyncListener annotationMapListener) {

        // TODO make this configurable from yml
        MapConfig mapConfig = config.getMapConfig("annotations:*");

        MapStoreConfig storeConfig = mapConfig.getMapStoreConfig();
        storeConfig.setEnabled(true);

        // CRITICAL: Pass the actual Spring-managed instance, NOT the class name string
        storeConfig.setImplementation(annotationMapStore);

        EntryListenerConfig listenerConfig = new EntryListenerConfig(annotationMapListener, true, true);
        mapConfig.addEntryListenerConfig(listenerConfig);

        // TODO make this configurable from yml

        log.info("Creating custom hazelcast instance");

        return Hazelcast.newHazelcastInstance(config);
    }
}

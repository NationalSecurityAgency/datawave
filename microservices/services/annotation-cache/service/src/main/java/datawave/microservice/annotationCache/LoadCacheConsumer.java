package datawave.microservice.annotationCache;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import datawave.annotation.protobuf.v1.Annotation;

@Component
public class LoadCacheConsumer implements HazelcastInstanceAware {
    private static Logger log = LoggerFactory.getLogger(LoadCacheConsumer.class);

    private HazelcastInstance hazelcastInstance;

    //
    // public LoadCacheConsumer(HazelcastInstance instance) {
    // this.instance = instance;
    // log.info("Wired up LoadCacheConsumer: " + instance);
    // }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        log.info("activated hzInstance: " + hazelcastInstance);
    }

    public LoadCacheConsumer() {
        log.info("created");
    }

    @Bean
    public Consumer<Annotation> loadCache() {
        return annotation -> {
            log.info("got annotation off queue: " + annotation.getAnnotationId());
        };
    }
}

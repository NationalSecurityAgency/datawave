package datawave.microservice.annotation.service.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.web.firewall.StrictHttpFirewall;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.JacksonAnnotationDeserializer;
import datawave.annotation.util.v1.JacksonAnnotationSerializer;
import datawave.annotation.util.v1.JacksonSegmentDeserializer;
import datawave.annotation.util.v1.JacksonSegmentSerializer;
import datawave.microservice.annotation.common.AnnotationSupplier;

@Configuration
@EnableConfigurationProperties(AnnotationProperties.class)
public class AnnotationServiceConfig {
    @Bean
    public AnnotationSupplier annotationSource() {
        return new AnnotationSupplier();
    }

    @Bean
    public StrictHttpFirewall httpFirewall() {
        // override the default strict http firewall to allow forward slashes. These are used in internal identifiers
        // as separators for shard/datatype/uid structures. We also allow shard:datatype:uid, but in some cases we
        // want to accept the slash variant which is returned by other services.
        StrictHttpFirewall firewall = new StrictHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        // this is due to strangeness in the StrictHttpFirewall, you must also allow encoded percents to allow encoded
        // slashes, arrived here via testing.
        firewall.setAllowUrlEncodedPercent(true);
        return firewall;
    }

    @Bean
    public Module jacksonAnnotationModule() {
        final SimpleModule simpleModule = new SimpleModule();

        // Added for Annotation and Segment serialization and deserialization.
        simpleModule.addDeserializer(Annotation.class, new JacksonAnnotationDeserializer());
        simpleModule.addSerializer(Annotation.class, new JacksonAnnotationSerializer());
        simpleModule.addDeserializer(Segment.class, new JacksonSegmentDeserializer());
        simpleModule.addSerializer(Segment.class, new JacksonSegmentSerializer());

        return simpleModule;
    }

    @Bean
    public ExecutorService federatedReadExecutorService() {
        return Executors.newCachedThreadPool();
    }
}

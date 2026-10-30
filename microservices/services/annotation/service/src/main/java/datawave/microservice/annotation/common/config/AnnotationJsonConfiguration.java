package datawave.microservice.annotation.common.config;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.JacksonAnnotationDeserializer;
import datawave.annotation.util.v1.JacksonAnnotationSerializer;
import datawave.annotation.util.v1.JacksonSegmentDeserializer;
import datawave.annotation.util.v1.JacksonSegmentSerializer;

/**
 * Keeps annotation JSON support available to both HTTP handling and queue processing, independently of the deployment role.
 */
@Configuration
public class AnnotationJsonConfiguration {
    @Bean
    public Module jacksonAnnotationModule() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Annotation.class, new JacksonAnnotationDeserializer());
        simpleModule.addSerializer(Annotation.class, new JacksonAnnotationSerializer());
        simpleModule.addDeserializer(Segment.class, new JacksonSegmentDeserializer());
        simpleModule.addSerializer(Segment.class, new JacksonSegmentSerializer());
        simpleModule.addSerializer(AnnotationSource.class, new JsonSerializer<>() {
            @Override
            public void serialize(AnnotationSource source, JsonGenerator generator, SerializerProvider provider) throws IOException {
                generator.writeRawValue(AnnotationJsonUtils.getPrinter().print(source));
            }
        });
        return simpleModule;
    }
}

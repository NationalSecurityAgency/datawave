package datawave.microservice.annotation.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

class AnnotationJsonConfigurationTest {
    @Test
    void standaloneSourceResponseUsesProtobufJsonAndPreservesMetadata() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new AnnotationJsonConfiguration().jacksonAnnotationModule());
        AnnotationSource source = AnnotationTestDataUtil.generateTestAnnotation().getSource();
        String json = mapper.writeValueAsString(source);
        AnnotationSource.Builder parsed = AnnotationSource.newBuilder();
        JsonFormat.parser().merge(json, parsed);
        assertThat(parsed.build()).isEqualTo(source);
        assertThat(mapper.readTree(json).has("unknownFields")).isFalse();
    }
}

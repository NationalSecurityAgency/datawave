package datawave.microservice.annotationCache.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Fails startup when effective configuration breaks the annotation-cache RabbitMQ contract. */
@Component
public class AnnotationMessagingConfigurationValidator {
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    public AnnotationMessagingConfigurationValidator(Environment environment) {
        List<String> errors = new ArrayList<>();

        requireNonBlank(environment, errors, "region.name");
        requireEquals(environment, errors, "spring.cloud.function.definition", "loadCache;busConsumer");
        requireEquals(environment, errors, "spring.rabbitmq.publisher-confirm-type", "correlated");

        requireEquals(environment, errors, "spring.cloud.stream.bindings.persisted-out-0.destination", "annotation");
        requireEquals(environment, errors, "spring.cloud.stream.bindings.persisted-out-0.content-type", PROTOBUF_CONTENT_TYPE);
        requireEquals(environment, errors, "spring.cloud.stream.rabbit.bindings.persisted-out-0.producer.delivery-mode", "PERSISTENT");
        requireEquals(environment, errors, "spring.cloud.stream.rabbit.bindings.persisted-out-0.producer.use-confirm-header", "true");

        requireEquals(environment, errors, "spring.cloud.stream.bindings.loadCache-in-0.destination", "annotation");
        requireEquals(environment, errors, "spring.cloud.stream.bindings.loadCache-in-0.group", "cache");
        requireEquals(environment, errors, "spring.cloud.stream.bindings.loadCache-in-0.content-type", PROTOBUF_CONTENT_TYPE);

        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid annotation-cache messaging configuration: " + String.join("; ", errors));
        }
    }

    private void requireNonBlank(Environment environment, List<String> errors, String property) {
        String value = environment.getProperty(property);
        if (value == null || value.isBlank()) {
            errors.add(property + " must be configured and nonblank");
        }
    }

    private void requireEquals(Environment environment, List<String> errors, String property, String expected) {
        String actual = environment.getProperty(property);
        if (actual == null || !expected.equals(actual.trim())) {
            errors.add(property + " must be '" + expected + "' (was '" + actual + "')");
        }
    }
}

package datawave.microservice.annotationCache.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class AnnotationMessagingConfigurationValidatorTest {
    @Test
    void acceptsRequiredAnnotationMessagingConfiguration() {
        assertDoesNotThrow(() -> new AnnotationMessagingConfigurationValidator(validEnvironment()));
    }

    @Test
    void rejectsMissingOrOverriddenRequiredSettingsTogether() {
        MockEnvironment environment = validEnvironment();
        environment.setProperty("spring.rabbitmq.publisher-confirm-type", "simple");
        environment.setProperty("spring.cloud.stream.bindings.loadCache-in-0.content-type", "application/json");
        environment.setProperty("spring.cloud.stream.bindings.persisted-out-0.destination", "wrong-exchange");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> new AnnotationMessagingConfigurationValidator(environment));

        assertTrue(exception.getMessage().contains("spring.rabbitmq.publisher-confirm-type"));
        assertTrue(exception.getMessage().contains("spring.cloud.stream.bindings.loadCache-in-0.content-type"));
        assertTrue(exception.getMessage().contains("spring.cloud.stream.bindings.persisted-out-0.destination"));
    }

    private MockEnvironment validEnvironment() {
        return new MockEnvironment().withProperty("region.name", "test-region").withProperty("spring.cloud.function.definition", "loadCache;busConsumer")
                        .withProperty("spring.rabbitmq.publisher-confirm-type", "correlated")
                        .withProperty("spring.cloud.stream.bindings.persisted-out-0.destination", "annotation")
                        .withProperty("spring.cloud.stream.bindings.persisted-out-0.content-type", "application/x-protobuf")
                        .withProperty("spring.cloud.stream.rabbit.bindings.persisted-out-0.producer.delivery-mode", "PERSISTENT")
                        .withProperty("spring.cloud.stream.rabbit.bindings.persisted-out-0.producer.use-confirm-header", "true")
                        .withProperty("spring.cloud.stream.bindings.loadCache-in-0.destination", "annotation")
                        .withProperty("spring.cloud.stream.bindings.loadCache-in-0.group", "cache")
                        .withProperty("spring.cloud.stream.bindings.loadCache-in-0.content-type", "application/x-protobuf");
    }
}

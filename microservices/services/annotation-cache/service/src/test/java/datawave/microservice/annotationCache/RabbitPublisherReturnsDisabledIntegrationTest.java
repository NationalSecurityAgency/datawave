package datawave.microservice.annotationCache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.test.binder.MessageCollectorAutoConfiguration;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * With publisher confirms enabled but publisher returns disabled, verifies whether the Rabbit binder still returns an unroutable mandatory publication through
 * confirm-header correlation. Enable with ANNOTATION_CACHE_RABBITMQ_IT=true and provide optional ANNOTATION_CACHE_RABBITMQ_HOST/PORT/USERNAME/PASSWORD
 * variables.
 */
@EnabledIfEnvironmentVariable(named = "ANNOTATION_CACHE_RABBITMQ_IT", matches = "true")
@SpringBootTest(classes = RabbitPublisherReturnsDisabledIntegrationTest.TestApplication.class,
                properties = {"spring.cloud.config.enabled=false", "spring.cloud.bus.enabled=false", "spring.cloud.function.definition=",
                        "spring.cloud.stream.defaultBinder=rabbit", "spring.rabbitmq.publisher-confirm-type=correlated",
                        "spring.rabbitmq.publisher-returns=false"})
class RabbitPublisherReturnsDisabledIntegrationTest {
    private static final String BINDING = "mandatory-without-returns-out-0";
    private static final String EXCHANGE = "annotation-cache-it-no-returns-" + UUID.randomUUID();

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private CachingConnectionFactory connectionFactory;

    @Autowired
    private BinderFactory binderFactory;

    @DynamicPropertySource
    static void rabbitAndBindingProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.rabbitmq.host", () -> env("ANNOTATION_CACHE_RABBITMQ_HOST", "localhost"));
        properties.add("spring.rabbitmq.port", () -> Integer.parseInt(env("ANNOTATION_CACHE_RABBITMQ_PORT", "5672")));
        properties.add("spring.rabbitmq.username", () -> env("ANNOTATION_CACHE_RABBITMQ_USERNAME", "guest"));
        properties.add("spring.rabbitmq.password", () -> env("ANNOTATION_CACHE_RABBITMQ_PASSWORD", "guest"));
        properties.add("spring.cloud.stream.bindings." + BINDING + ".destination", () -> EXCHANGE);
        properties.add("spring.cloud.stream.bindings." + BINDING + ".content-type", () -> "text/plain");
        properties.add("spring.cloud.stream.rabbit.bindings." + BINDING + ".producer.use-confirm-header", () -> true);
    }

    @Test
    void publisherReturnsDisabledStillReturnsMandatoryPublishThroughConfirmHeader() throws Exception {
        RabbitMessageChannelBinder rabbitBinder = (RabbitMessageChannelBinder) binderFactory.getBinder("rabbit", MessageChannel.class);
        assertSame(connectionFactory, rabbitBinder.getConnectionFactory(), "the assertion must cover the Rabbit binder's active connection factory");
        assertTrue(connectionFactory.isPublisherConfirms());
        assertFalse(connectionFactory.isPublisherReturns());

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        Message<byte[]> message = MessageBuilder.withPayload("unroutable-test".getBytes(StandardCharsets.UTF_8))
                        .setHeader(AnnotationMapStore.AMQP_CORRELATION_DATA_HEADER, correlationData)
                        .setHeader(AnnotationMapStore.AMQP_PUBLISH_CONFIRM_CORRELATION_HEADER, correlationData).build();

        assertTrue(streamBridge.send(BINDING, message));
        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);

        assertTrue(confirm.isAck(), "RabbitMQ ACKs the mandatory publish at the exchange");
        assertNotNull(correlationData.getReturned(), "the Rabbit binder reports the mandatory return through confirm-header correlation");
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {TestSupportBinderAutoConfiguration.class, MessageCollectorAutoConfiguration.class})
    static class TestApplication {}
}

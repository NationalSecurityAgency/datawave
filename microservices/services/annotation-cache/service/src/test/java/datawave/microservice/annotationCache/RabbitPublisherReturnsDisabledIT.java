package datawave.microservice.annotationCache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * With publisher confirms enabled but publisher returns disabled, verifies whether the Rabbit binder still returns an unroutable mandatory publication through
 * confirm-header correlation. A RabbitMQ Testcontainer starts for this test; the test is skipped when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = RabbitPublisherReturnsDisabledIT.TestApplication.class,
                properties = {"spring.cloud.config.enabled=false", "spring.cloud.bus.enabled=false", "spring.cloud.function.definition=",
                        "spring.cloud.stream.defaultBinder=rabbit", "spring.rabbitmq.publisher-confirm-type=correlated",
                        "spring.rabbitmq.publisher-returns=false"})
class RabbitPublisherReturnsDisabledIT {
    private static final String BINDING = "mandatory-without-returns-out-0";
    private static final String EXCHANGE = "annotation-cache-it-no-returns-" + UUID.randomUUID();

    @Container
    static final RabbitMQContainer rabbit = RabbitMqIntegrationTestSupport.newRabbitContainer();

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private CachingConnectionFactory connectionFactory;

    @Autowired
    private BinderFactory binderFactory;

    @DynamicPropertySource
    static void rabbitAndBindingProperties(DynamicPropertyRegistry properties) {
        RabbitMqIntegrationTestSupport.registerRabbitProperties(rabbit, properties);
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

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {TestSupportBinderAutoConfiguration.class, MessageCollectorAutoConfiguration.class})
    static class TestApplication {}
}

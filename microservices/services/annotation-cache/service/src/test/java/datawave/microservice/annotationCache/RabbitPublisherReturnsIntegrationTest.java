package datawave.microservice.annotationCache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.test.binder.MessageCollectorAutoConfiguration;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Exercises Rabbit binder mandatory publishing against a real broker. Enable with ANNOTATION_CACHE_RABBITMQ_IT=true and provide optional
 * ANNOTATION_CACHE_RABBITMQ_HOST/PORT/USERNAME/PASSWORD environment variables. The tests publish to exchanges with no queue bound.
 */
@EnabledIfEnvironmentVariable(named = "ANNOTATION_CACHE_RABBITMQ_IT", matches = "true")
@SpringBootTest(classes = RabbitPublisherReturnsIntegrationTest.TestApplication.class,
                properties = {"spring.cloud.config.enabled=false", "spring.cloud.bus.enabled=false", "spring.cloud.function.definition=",
                        "spring.cloud.stream.defaultBinder=rabbit", "spring.rabbitmq.publisher-confirm-type=correlated",
                        "spring.rabbitmq.publisher-returns=true"})
class RabbitPublisherReturnsIntegrationTest {
    private static final String ERROR_CHANNEL_BINDING = "returns-enabled-out-0";
    private static final String NO_ERROR_CHANNEL_BINDING = "returns-disabled-out-0";
    private static final String CONFIRM_HEADER_BINDING = "confirm-header-out-0";
    private static final String STORE_BINDING = "persisted-out-0";
    private static final String ERROR_CHANNEL_EXCHANGE = "annotation-cache-it-return-enabled";
    private static final String NO_ERROR_CHANNEL_EXCHANGE = "annotation-cache-it-return-disabled";
    private static final String CONFIRM_HEADER_EXCHANGE = "annotation-cache-it-confirm-header";
    private static final String STORE_EXCHANGE = "annotation-cache-it-store-correlation";

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private BindingServiceProperties bindingServiceProperties;

    @Autowired
    private CachingConnectionFactory connectionFactory;

    @Autowired
    @Qualifier("errorChannel")
    private SubscribableChannel globalErrorChannel;

    @Autowired
    private BlockingQueue<Message<?>> returnedMessages;

    private final MessageHandler returnedMessageHandler = message -> returnedMessages.add(message);

    @BeforeEach
    void subscribeToBinderErrorChannel() {
        returnedMessages.clear();
        globalErrorChannel.subscribe(returnedMessageHandler);
    }

    @AfterEach
    void unsubscribeFromBinderErrorChannel() {
        globalErrorChannel.unsubscribe(returnedMessageHandler);
        returnedMessages.clear();
    }

    @DynamicPropertySource
    static void rabbitAndBindingProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.rabbitmq.host", () -> env("ANNOTATION_CACHE_RABBITMQ_HOST", "localhost"));
        properties.add("spring.rabbitmq.port", () -> Integer.parseInt(env("ANNOTATION_CACHE_RABBITMQ_PORT", "5672")));
        properties.add("spring.rabbitmq.username", () -> env("ANNOTATION_CACHE_RABBITMQ_USERNAME", "guest"));
        properties.add("spring.rabbitmq.password", () -> env("ANNOTATION_CACHE_RABBITMQ_PASSWORD", "guest"));

        addOutputBinding(properties, ERROR_CHANNEL_BINDING, ERROR_CHANNEL_EXCHANGE, true, false);
        addOutputBinding(properties, NO_ERROR_CHANNEL_BINDING, NO_ERROR_CHANNEL_EXCHANGE, false, false);
        addOutputBinding(properties, CONFIRM_HEADER_BINDING, CONFIRM_HEADER_EXCHANGE, false, true);
        addOutputBinding(properties, STORE_BINDING, STORE_EXCHANGE, false, true);
    }

    /** Verifies that enabling the producer error channel makes an unroutable publication mandatory and returns it. */
    @Test
    void errorChannelEnabledReturnsUnroutablePublication() throws Exception {
        assertTrue(bindingServiceProperties.getProducerProperties(ERROR_CHANNEL_BINDING).isErrorChannelEnabled());
        assertTrue(connectionFactory.isPublisherReturns());

        assertTrue(streamBridge.send(ERROR_CHANNEL_BINDING, payload()));
        Message<?> returned = returnedMessages.poll(10, TimeUnit.SECONDS);
        assertNotNull(returned, "RabbitMQ should return an unroutable mandatory publication to the binder error channel");
    }

    /** Verifies that without the producer error channel, an unroutable publication is ACKed but silently discarded. */
    @Test
    void disabledErrorChannelSilentlyDropsUnroutablePublication() throws Exception {
        CorrelationData correlationData = correlationData("unroutable-without-error-channel");

        assertTrue(streamBridge.send(NO_ERROR_CHANNEL_BINDING, correlatedPayload(correlationData)));

        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertTrue(confirm.isAck(), "the exchange ACKs a publication even when it routes to no queue");
        assertNull(correlationData.getReturned(), "without mandatory publishing, RabbitMQ silently drops the unroutable message");
        assertNull(returnedMessages.poll(1, TimeUnit.SECONDS), "the binding without error-channel-enabled receives no return event");
    }

    /** Verifies that use-confirm-header alone enables a mandatory publish and correlates its return without a producer error channel. */
    @Test
    void confirmHeaderReturnsUnroutablePublicationWithoutErrorChannel() throws Exception {
        CorrelationData correlationData = correlationData("unroutable-confirm-header-publication");

        assertTrue(streamBridge.send(CONFIRM_HEADER_BINDING, correlatedPayload(correlationData)));

        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertTrue(confirm.isAck(), "RabbitMQ ACKs the exchange publication even though it is unroutable");
        assertNotNull(correlationData.getReturned(), "use-confirm-header makes the publication mandatory and stores the return on CorrelationData");
    }

    /** Verifies the MapStore correlation contract: a returned message is attached to CorrelationData even when the broker ACKs the publish. */
    @Test
    void storeCorrelationDataContainsReturnAlongsideBrokerAck() throws Exception {
        assertFalse(bindingServiceProperties.getProducerProperties(STORE_BINDING).isErrorChannelEnabled(),
                        "the MapStore uses confirm-header correlation and does not need the producer error channel");
        CorrelationData correlationData = correlationData("unroutable-store-publication");

        assertTrue(streamBridge.send(STORE_BINDING, correlatedPayload(correlationData)));

        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertTrue(confirm.isAck(), "RabbitMQ ACKs the publish even though it cannot route it to a queue");
        assertNotNull(correlationData.getReturned(), "AnnotationMapStore must reject the returned publication despite the ACK");
        assertNull(returnedMessages.poll(1, TimeUnit.SECONDS),
                        "confirm-header correlation returns the result on CorrelationData without a binder error channel");
    }

    private Message<byte[]> payload() {
        return MessageBuilder.withPayload("unroutable-test".getBytes(StandardCharsets.UTF_8)).build();
    }

    private Message<byte[]> correlatedPayload(CorrelationData correlationData) {
        return MessageBuilder.withPayload("unroutable-test".getBytes(StandardCharsets.UTF_8))
                        .setHeader(AnnotationMapStore.AMQP_CORRELATION_DATA_HEADER, correlationData)
                        .setHeader(AnnotationMapStore.AMQP_PUBLISH_CONFIRM_CORRELATION_HEADER, correlationData).build();
    }

    private CorrelationData correlationData(String id) {
        return new CorrelationData(id);
    }

    private static void addOutputBinding(DynamicPropertyRegistry properties, String binding, String exchange, boolean errorChannelEnabled,
                    boolean useConfirmHeader) {
        properties.add("spring.cloud.stream.bindings." + binding + ".destination", () -> exchange);
        properties.add("spring.cloud.stream.bindings." + binding + ".content-type", () -> "text/plain");
        properties.add("spring.cloud.stream.bindings." + binding + ".producer.error-channel-enabled", () -> errorChannelEnabled);
        properties.add("spring.cloud.stream.rabbit.bindings." + binding + ".producer.use-confirm-header", () -> useConfirmHeader);
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {TestSupportBinderAutoConfiguration.class, MessageCollectorAutoConfiguration.class})
    static class TestApplication {
        @Bean
        BlockingQueue<Message<?>> returnedMessages() {
            return new LinkedBlockingQueue<>();
        }
    }
}

package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.integration.IntegrationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ClassUtils;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.messaging.AnnotationMessagingConfiguration;
import datawave.microservice.annotation.service.AnnotationPublisher;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.writers.log.config.LogAnnotationWriterProperties;

/**
 * Initializes the real Spring Cloud function catalog and stream binding lifecycle with a recording binder instead of an external RabbitMQ broker.
 */
class AnnotationStreamRoleTest {
    private ApplicationContextRunner streams(RecordingBinder binder) {
        BinderFactory factory = mock(BinderFactory.class);
        doReturn(binder).when(factory).getBinder(any(), any());
        return new ApplicationContextRunner()
                        .withConfiguration(AutoConfigurations.of(IntegrationAutoConfiguration.class, ContextFunctionCatalogAutoConfiguration.class,
                                        ClassUtils.resolveClassName("org.springframework.cloud.function.cloudevent.CloudEventsFunctionExtensionConfiguration",
                                                        getClass().getClassLoader()),
                                        BindingServiceConfiguration.class, FunctionConfiguration.class))
                        .withUserConfiguration(AnnotationRoleConfiguration.class, AnnotationServiceConfig.class, AnnotationJsonConfiguration.class,
                                        AnnotationMessagingConfiguration.class, LogAnnotationWriterProperties.class)
                        .withBean(BinderFactory.class, () -> factory).withPropertyValues("spring.cloud.bus.enabled=false",
                                        "annotation.writers.log.enabled=true", "spring.cloud.stream.bindings.annotationSource-out-0.destination=annotation",
                                        "spring.cloud.stream.bindings.annotationSource-out-0.content-type=application/x-protobuf",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.destination=annotation",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.content-type=application/x-protobuf",
                                        "spring.cloud.stream.bindings.logAnnotationSink-in-0.group=log");
    }

    @ParameterizedTest
    @CsvSource({"api,1,0", "writer,0,1", "combined,1,1"})
    void roleSelectsOnlyItsMessagingResponsibilities(String role, int producers, int consumers) {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-" + role, "annotation.deployment.overlays.annotation-" + role + "=true")
                        .run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(binder.producers).isEqualTo(producers);
                            assertThat(binder.consumers).isEqualTo(consumers);
                            assertThat(context.containsBean("annotationSource-out-0")).isEqualTo(producers == 1);
                            assertThat(context.containsBean("logAnnotationSink-in-0")).isEqualTo(consumers == 1);
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"spring.cloud.function.definition", "spring.cloud.stream.function.definition"})
    void functionOverridesFailBeforeBindingsInitialize(String property) {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-api", "annotation.deployment.overlays.annotation-api=true",
                        property + "=annotationSource").run(context -> {
                            assertThat(context).hasFailed();
                            assertThat(context.getStartupFailure()).hasMessageContaining(property + " is managed by the annotation service");
                            assertThat(binder.producers).isZero();
                            assertThat(binder.consumers).isZero();
                        });
    }

    @ParameterizedTest
    @MethodSource("bindingAliases")
    void aliasesForFixedAnnotationBindingsFailBeforeBindingsInitialize(String role, String binding, boolean brackets) {
        RecordingBinder binder = new RecordingBinder();
        String property = "spring.cloud.stream.function.bindings." + (brackets ? "[" + binding + "]" : binding);
        streams(binder).withPropertyValues("spring.profiles.active=annotation-" + role, "annotation.deployment.overlays.annotation-" + role + "=true",
                        property + "=renamed").run(context -> {
                            assertThat(context).hasFailed();
                            assertThat(context.getStartupFailure()).hasMessageContaining("must not rename the fixed annotation binding")
                                            .hasMessageContaining(binding);
                            assertThat(binder.producers).isZero();
                            assertThat(binder.consumers).isZero();
                        });
    }

    private static Stream<Arguments> bindingAliases() {
        return Stream.of("api", "writer", "combined")
                        .flatMap(role -> Stream.of("annotationSource-out-0", "logAnnotationSink-in-0", "accumuloAnnotationSink-in-0", "dumpAnnotationSink-in-0")
                                        .flatMap(binding -> Stream.of(false, true).map(brackets -> Arguments.of(role, binding, brackets))));
    }

    @Test
    void identityAliasesPreserveBindingsAndConfirmationConversion() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-combined", "annotation.deployment.overlays.annotation-combined=true",
                        "spring.cloud.stream.function.bindings.annotationSource-out-0=annotationSource-out-0",
                        "spring.cloud.stream.function.bindings.logAnnotationSink-in-0=logAnnotationSink-in-0").run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(binder.producers).isEqualTo(1);
                            assertThat(binder.consumers).isEqualTo(1);
                            context.publishEvent(new BindingCreatedEvent(binder.producerBinding));
                            AnnotationMessage expected = AnnotationMessage.newBuilder().setAnnotationMessageId("identity-binding").build();
                            CorrelationData correlation = new CorrelationData("identity-binding-confirmation");
                            assertThat(context.getBean(AnnotationSupplier.class)
                                            .send(new GenericMessage<>(expected, Map.of(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, correlation)))).isTrue();
                            Message<?> wire = binder.messages.poll(3, TimeUnit.SECONDS);
                            assertThat(wire).isNotNull();
                            assertThat(AnnotationMessage.parseFrom((byte[]) wire.getPayload())).isEqualTo(expected);
                            assertThat(wire.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION)).isSameAs(correlation);
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"yes"})
    void noncanonicalTrueFlagFailsBeforeBindingsInitialize(String value) {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-writer", "annotation.deployment.overlays.annotation-writer=true",
                        "annotation.writers.log.enabled=" + value).run(context -> {
                            assertThat(context).hasFailed();
                            assertThat(context.getStartupFailure())
                                            .hasMessage("annotation.writers.log.enabled must be true or false; received '" + value + "'");
                            assertThat(binder.producers).isZero();
                            assertThat(binder.consumers).isZero();
                        });
    }

    @ParameterizedTest
    @CsvSource({"true,1", "TRUE,1", "TrUe,1", "false,0", "FALSE,0", "FaLsE,0"})
    void canonicalFlagsAgreeWithActualConsumerBeansAndBindings(String value, int expectedConsumers) {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-combined", "annotation.deployment.overlays.annotation-combined=true",
                        "annotation.writers.log.enabled=" + value).run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(binder.producers).isEqualTo(1);
                            assertThat(binder.consumers).isEqualTo(expectedConsumers);
                            assertThat(context.containsBean("logAnnotationSink")).isEqualTo(expectedConsumers == 1);
                            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition").contains("logAnnotationSink"))
                                            .isEqualTo(expectedConsumers == 1);
                        });
    }

    @Test
    void confirmationHeaderSurvivesOutboundProtobufConversion() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-api", "annotation.deployment.overlays.annotation-api=true",
                        "annotation.annotation-ack-timeout-millis=5000").run(context -> {
                            assertThat(context).hasNotFailed();
                            context.publishEvent(new BindingCreatedEvent(binder.producerBinding));
                            AnnotationMessage expected = AnnotationMessage.newBuilder().setAnnotationMessageId("converted-confirm")
                                            .addAnnotations(AnnotationTestDataUtil.generateTestAnnotation()).build();
                            ExecutorService executor = Executors.newSingleThreadExecutor();
                            try {
                                var result = executor.submit(() -> context.getBean(AnnotationPublisher.class).publishAndConfirm(expected));
                                Message<?> wire = binder.messages.poll(3, TimeUnit.SECONDS);
                                assertThat(wire).isNotNull();
                                assertThat(wire.getPayload()).isInstanceOf(byte[].class);
                                assertThat(AnnotationMessage.parseFrom((byte[]) wire.getPayload())).isEqualTo(expected);
                                CorrelationData correlation = wire.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, CorrelationData.class);
                                assertThat(correlation).isNotNull();
                                correlation.getFuture().set(new CorrelationData.Confirm(true, null));
                                assertThat(result.get(3, TimeUnit.SECONDS)).contains(expected);
                            } finally {
                                executor.shutdownNow();
                            }
                        });
    }

    @Test
    void streamBindingsSerializeAndConsumeProtobufBytes() {
        RecordingBinder binder = new RecordingBinder();
        streams(binder).withPropertyValues("spring.profiles.active=annotation-combined", "annotation.deployment.overlays.annotation-combined=true")
                        .run(context -> {
                            assertThat(context).hasNotFailed().hasBean("annotationProtobufMessageConverter");
                            AnnotationMessage expected = AnnotationMessage.newBuilder().setSource("annotation-role-conversion-test")
                                            .addAnnotations(AnnotationTestDataUtil.generateTestAnnotation()).build();
                            // Rabbit's binder publishes this event to start the reactive supplier.
                            context.publishEvent(new BindingCreatedEvent(binder.producerBinding));
                            assertThat(context.getBean(AnnotationSupplier.class).send(new GenericMessage<>(expected))).isTrue();

                            Message<?> wire = binder.messages.poll(3, TimeUnit.SECONDS);
                            assertThat(wire).isNotNull();
                            assertThat(wire.getPayload()).isInstanceOf(byte[].class);
                            assertThat(AnnotationMessage.parseFrom((byte[]) wire.getPayload())).isEqualTo(expected);
                            // Deliver broker-shaped bytes through the real inbound conversion and function invocation chain.
                            assertThat(binder.consumerChannel
                                            .send(new GenericMessage<>(wire.getPayload(), Map.of(MessageHeaders.CONTENT_TYPE, "application/x-protobuf"))))
                                            .isTrue();
                        });
    }

    static class RecordingBinder implements Binder<MessageChannel,ConsumerProperties,ProducerProperties> {
        int producers;
        int consumers;
        final BlockingQueue<Message<?>> messages = new LinkedBlockingQueue<>();
        MessageChannel consumerChannel;
        Binding<MessageChannel> producerBinding;

        @Override
        @SuppressWarnings("unchecked")
        public Binding<MessageChannel> bindConsumer(String destination, String group, MessageChannel channel, ConsumerProperties properties) {
            assertThat(destination).isEqualTo("annotation");
            assertThat(group).isEqualTo("log");
            consumers++;
            consumerChannel = channel;
            return mock(Binding.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Binding<MessageChannel> bindProducer(String destination, MessageChannel channel, ProducerProperties properties) {
            assertThat(destination).isEqualTo("annotation");
            producers++;
            ((SubscribableChannel) channel).subscribe(messages::add);
            producerBinding = mock(Binding.class);
            return producerBinding;
        }
    }
}

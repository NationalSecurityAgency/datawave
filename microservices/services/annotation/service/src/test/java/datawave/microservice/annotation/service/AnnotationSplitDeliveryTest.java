package datawave.microservice.annotation.service;

import static datawave.annotation.test.v1.AnnotationAssertions.assertAnnotationSourcesEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.messaging.Message;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.AnnotationConsumer;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.config.AnnotationRoleConfiguration;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.accumulo.AccumuloAnnotationWriter;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;
import reactor.core.Disposable;

/**
 * Uses independent API and writer contexts with simulated broker confirms and queued protobuf transport. Persistence is deliberately deferred until after API
 * acceptance, so this test also guards the asynchronous acceptance boundary. Live RabbitMQ routing and competing consumers require an integration environment.
 */
class AnnotationSplitDeliveryTest {
    @Test
    void confirmedApiWriteIsConsumedAndPersistedBySeparateWriterContext() throws Exception {
        AccumuloClient client = new InMemoryAccumuloClient("writer", new InMemoryInstance("annotation-split-" + UUID.randomUUID()));
        client.securityOperations().changeUserAuthorizations("writer", new Authorizations("PUBLIC", "PRIVATE"));
        AccumuloConnectionFactory writerFactory = Mockito.mock(AccumuloConnectionFactory.class);
        Mockito.when(writerFactory.getClient(anyString(), anyCollection(), anyString(), any(), anyMap())).thenReturn(client);

        ApplicationContextRunner writer = new ApplicationContextRunner()
                        .withUserConfiguration(AnnotationRoleConfiguration.class, AnnotationSerializerConfiguration.class, AccumuloAnnotationWriterConfig.class,
                                        AnnotationServiceConfig.class, AnnotationControllerV1.class)
                        .withBean(AccumuloConnectionFactory.class, () -> writerFactory).withPropertyValues("spring.profiles.active=writer",
                                        "annotation.deployment.overlays.writer=true", "annotation.writers.accumulo.enabled=true",
                                        "annotation.writers.accumulo.health.enabled=true", "spring.cloud.bus.enabled=false");
        ApplicationContextRunner api = new ApplicationContextRunner()
                        .withUserConfiguration(AnnotationRoleConfiguration.class, AnnotationSerializerConfiguration.class, AnnotationServiceConfig.class,
                                        AnnotationControllerV1.class, AccumuloAnnotationWriterConfig.class)
                        .withBean(AccumuloConnectionFactory.class, () -> Mockito.mock(AccumuloConnectionFactory.class))
                        .withBean(LookupService.class, () -> Mockito.mock(LookupService.class)).withPropertyValues("spring.profiles.active=api",
                                        "annotation.deployment.overlays.api=true", "annotation.writers.accumulo.enabled=true",
                                        "annotation.annotation-ack-timeout-millis=2000", "annotation.retry.max-attempts=1", "spring.cloud.bus.enabled=false");

        try {
            writer.run(writerContext -> {
                assertThat(writerContext).hasNotFailed().hasBean("accumuloAnnotationSink").hasBean("accumuloHealthChecker")
                                .doesNotHaveBean(AnnotationControllerV1.class).doesNotHaveBean("annotationSource");
                AnnotationConsumer consumer = writerContext.getBean("accumuloAnnotationSink", AnnotationConsumer.class);
                AnnotationDataAccess stored = writerContext.getBean(AccumuloAnnotationWriter.class).getDataAccess();

                api.run(apiContext -> {
                    assertThat(apiContext).hasNotFailed().hasSingleBean(AnnotationControllerV1.class).doesNotHaveBean("accumuloAnnotationWriter")
                                    .doesNotHaveBean("accumuloAnnotationSink");
                    AnnotationControllerV1 controller = apiContext.getBean(AnnotationControllerV1.class);
                    BlockingQueue<Message<AnnotationMessage>> brokerQueue = new LinkedBlockingQueue<>();
                    Disposable producer = apiContext.getBean(AnnotationSupplier.class).get().subscribe(message -> {
                        brokerQueue.add(message);
                        message.getHeaders()
                                        .get(org.springframework.amqp.support.AmqpHeaders.PUBLISH_CONFIRM_CORRELATION,
                                                        org.springframework.amqp.rabbit.connection.CorrelationData.class)
                                        .getFuture().set(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(true, null));
                    });
                    try {
                        Optional<Annotation> accepted = controller.writeAnnotation(AnnotationTestDataUtil.generateTestAnnotation());
                        assertThat(accepted).isPresent();
                        Annotation expected = accepted.orElseThrow();
                        assertThat(apiContext.getBean(AnnotationPublisher.class).isEmpty()).isTrue();
                        assertThat(stored.getAnnotation(expected.getShard(), expected.getDataType(), expected.getUid(), expected.getAnnotationId())).isEmpty();

                        Message<AnnotationMessage> queued = brokerQueue.poll(2, TimeUnit.SECONDS);
                        assertThat(queued).isNotNull();
                        consumer.accept(AnnotationMessage.parseFrom(queued.getPayload().toByteArray()));

                        Optional<Annotation> persisted = stored.getAnnotation(expected.getShard(), expected.getDataType(), expected.getUid(),
                                        expected.getAnnotationId());
                        assertThat(persisted).isPresent();
                        assertThat(persisted.orElseThrow().getSegmentsList()).isEqualTo(expected.getSegmentsList());
                        Optional<AnnotationSource> source = stored.getAnnotationSource(expected.getAnalyticSourceHash());
                        assertThat(source).isPresent();
                        assertAnnotationSourcesEqual(expected.getSource(), source.orElseThrow());
                        Mockito.verifyNoInteractions(apiContext.getBean(AccumuloConnectionFactory.class));
                    } finally {
                        producer.dispose();
                    }
                });
            });
            Mockito.verify(writerFactory).returnClient(client);
        } finally {
            client.close();
        }
    }
}

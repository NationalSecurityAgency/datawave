package datawave.microservice.annotation.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.core.context.SecurityContextHolder;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import reactor.core.Disposable;

class AnnotationPublisherSecurityTest {
    @Test
    void brokerConfirmationCompletesWithoutRestAuthentication() {
        new ApplicationContextRunner()
                        .withUserConfiguration(MethodSecurity.class, AnnotationControllerV1.class, AnnotationServiceConfig.class,
                                        AnnotationSerializerConfiguration.class)
                        .withPropertyValues("spring.profiles.active=annotation-combined", "annotation.annotation-ack-timeout-millis=5000")
                        .withBean(AccumuloConnectionFactory.class, () -> mock(AccumuloConnectionFactory.class))
                        .withBean(LookupService.class, () -> mock(LookupService.class)).run(context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(AopUtils.isAopProxy(context.getBean(AnnotationControllerV1.class))).isTrue();
                            ExecutorService brokerThread = Executors.newSingleThreadExecutor();
                            Disposable subscription = context.getBean(AnnotationSupplier.class).get().subscribe(sent -> {
                                brokerThread.submit(() -> {
                                    SecurityContextHolder.clearContext();
                                    assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
                                    sent.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, CorrelationData.class).getFuture()
                                                    .set(new CorrelationData.Confirm(true, null));
                                });
                            });
                            try {
                                SecurityContextHolder.clearContext();
                                AnnotationPublisher publisher = context.getBean(AnnotationPublisher.class);
                                assertThat(publisher.publishAndConfirm(AnnotationMessage.newBuilder().setAnnotationMessageId("broker-confirm").build()))
                                                .isPresent();
                                assertThat(publisher.isEmpty()).isTrue();
                            } finally {
                                subscription.dispose();
                                brokerThread.shutdownNow();
                                assertThat(brokerThread.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                                SecurityContextHolder.clearContext();
                            }
                        });
    }

    @Configuration
    @EnableGlobalMethodSecurity(securedEnabled = true)
    static class MethodSecurity {}
}

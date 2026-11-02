package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.bus.BusEnvironmentPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.config.AccumuloConfiguration;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.common.config.AnnotationSerializerConfiguration;
import datawave.microservice.annotation.service.AnnotationControllerV1;
import datawave.microservice.annotation.service.AnnotationPublisher;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.service.config.AnnotationServiceConfig;
import datawave.microservice.annotation.util.lookup.common.LookupRequest;
import datawave.microservice.annotation.util.lookup.config.HttpClientConfig;
import datawave.microservice.annotation.util.lookup.config.LookupConfig;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.DumpAnnotationWriterConfig;
import datawave.microservice.annotation.writers.file.config.FileAnnotationWriterConfig;
import datawave.microservice.annotation.writers.log.config.LogAnnotationWriterProperties;

class AnnotationRoleConfigurationTest {
    @TempDir
    java.nio.file.Path files;

    // @formatter:off
    private final ApplicationContextRunner roles = new ApplicationContextRunner()
            .withUserConfiguration(RoleComponents.class)
            .withInitializer(context -> {
                ConfigurationPropertySources.attach(context.getEnvironment());
                new BusEnvironmentPostProcessor().postProcessEnvironment(context.getEnvironment(), new SpringApplication(RoleComponents.class));
            });
    // @formatter:on

    @Test
    void apiBootsWithComponentScannedProperties() {
        // @formatter:off
        roles
                .withUserConfiguration(ApiDependencies.class, AnnotationProperties.class)
                .withPropertyValues(
                        "spring.profiles.active=api",
                        "annotation.deployment.overlays.api=true",
                        "annotation.annotation-ack-timeout-millis=1234")
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasSingleBean(AnnotationProperties.class)
                            .hasSingleBean(AnnotationPublisher.class)
                            .hasSingleBean(AnnotationControllerV1.class);
                    assertThat(context.getBean(AnnotationProperties.class).getAnnotationAckTimeoutMillis()).isEqualTo(1234);
                });
        // @formatter:on
    }

    @Test
    void apiRoleDoesNotInitializeAnyQueueWriterEvenWhenSharedConfigEnablesThem() {
        // @formatter:off
        roles
                .withUserConfiguration(ApiDependencies.class)
                .withPropertyValues(
                        "spring.profiles.active=api",
                        "annotation.deployment.overlays.api=true",
                        "annotation.writers.accumulo.enabled=true",
                        "annotation.writers.log.enabled=true",
                        "annotation.writers.dump.enabled=true",
                        "annotation.writers.file.enabled=false")
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasSingleBean(AnnotationControllerV1.class)
                            .hasSingleBean(AnnotationPublisher.class)
                            .hasSingleBean(LookupService.class)
                            .hasSingleBean(ExecutorService.class)
                            .hasBean("annotationSource")
                            .doesNotHaveBean("accumuloAnnotationWriter")
                            .doesNotHaveBean("accumuloAnnotationSink")
                            .doesNotHaveBean("logAnnotationSink")
                            .doesNotHaveBean("dumpAnnotationSink")
                            .doesNotHaveBean("fileAnnotationWriter")
                            .doesNotHaveBean("accumuloHealthChecker");
                    assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                            .isEqualTo("annotationSource;busConsumer");
                    Mockito.verifyNoInteractions(context.getBean(AccumuloConnectionFactory.class));
                });
        // @formatter:on
    }

    @Test
    void writerRoleStartsWithoutApiDependenciesOrDirectFileFallback() {
        // @formatter:off
        roles
                .withUserConfiguration(AccumuloConfiguration.class)
                .withPropertyValues(
                        "spring.profiles.active=writer",
                        "annotation.deployment.overlays.writer=true",
                        "annotation.writers.log.enabled=true",
                        "annotation.writers.file.enabled=true",
                        "annotation.writers.file.pathUri=invalid://must-not-be-created")
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasBean("logAnnotationWriter")
                            .hasBean("logAnnotationSink")
                            .hasBean("jacksonAnnotationModule")
                            .doesNotHaveBean(AnnotationControllerV1.class)
                            .doesNotHaveBean(AnnotationPublisher.class)
                            .doesNotHaveBean(LookupService.class)
                            .doesNotHaveBean(LookupRequest.class)
                            .doesNotHaveBean("httpClient")
                            .doesNotHaveBean("lookupProperties")
                            .doesNotHaveBean("annotationSource")
                            .doesNotHaveBean("federatedReadExecutorService")
                            .doesNotHaveBean("fileAnnotationWriter")
                            .doesNotHaveBean(AccumuloConnectionFactory.class)
                            .doesNotHaveBean("connectionPoolsProperties");
                    assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                            .isEqualTo("logAnnotationSink;busConsumer");
                });
        // @formatter:on
    }

    @Test
    void explicitCombinedRoleRetainsApiAndConfiguredConsumersAndFileFallback() {
        // @formatter:off
        roles
                .withUserConfiguration(ApiDependencies.class)
                .withPropertyValues(
                        "spring.profiles.active=combined",
                        "annotation.deployment.overlays.combined=true",
                        "annotation.writers.log.enabled=true",
                        "annotation.writers.file.enabled=true",
                        "annotation.writers.file.pathUri=" + files.toUri())
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasSingleBean(AnnotationControllerV1.class)
                            .hasBean("annotationSource")
                            .hasBean("logAnnotationSink")
                            .hasBean("fileAnnotationWriter");
                    assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                            .isEqualTo("annotationSource;logAnnotationSink;busConsumer");
                });
        // @formatter:on
    }

    @Test
    void dumpConsumerProvidesQueueDrivenFileOutputInWriterRole() {
        // @formatter:off
        roles
                .withUserConfiguration(AccumuloConfiguration.class)
                .withPropertyValues(
                        "spring.profiles.active=writer",
                        "annotation.deployment.overlays.writer=true",
                        "annotation.writers.accumulo.enabled=false",
                        "annotation.writers.dump.enabled=true",
                        "annotation.writers.dump.pathUri=" + files.toUri(),
                        "spring.cloud.bus.enabled=false")
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasBean("dumpAnnotationSink")
                            .hasBean("dumpAnnotationWriter")
                            .doesNotHaveBean("annotationSource")
                            .doesNotHaveBean("fileAnnotationWriter")
                            .doesNotHaveBean(AccumuloConnectionFactory.class)
                            .doesNotHaveBean("connectionPoolsProperties");
                    assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition")).isEqualTo("dumpAnnotationSink");
                });
        // @formatter:on
    }

    @Test
    void writerWithoutEnabledQueueConsumerFailsClearly() {
        // @formatter:off
        roles
                .withPropertyValues(
                        "spring.profiles.active=writer",
                        "annotation.deployment.overlays.writer=true",
                        "annotation.writers.file.enabled=true")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessage("Profile writer requires an enabled log, accumulo, or dump queue consumer");
                });
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("invalidQueueWriterFlags")
    void invalidQueueWriterFlagsFailEvenWithOtherBackendsEnabled(String backend, String value) {
        Map<String,Object> flags = new HashMap<>();
        flags.put("annotation.writers.log.enabled", true);
        flags.put("annotation.writers.dump.enabled", true);
        flags.put("annotation.writers.accumulo.enabled", false);
        String property = "annotation.writers." + backend + ".enabled";
        flags.put(property, value);
        for (String role : new String[] {"api", "writer", "combined"}) {
            // @formatter:off
            roles
                    .withUserConfiguration(ApiDependencies.class, AccumuloConfiguration.class)
                    .withPropertyValues(
                            "spring.profiles.active=" + role,
                            "annotation.deployment.overlays." + role + "=true",
                            "annotation.writers.dump.pathUri=" + files.toUri())
                    .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("backend-flags", flags)))
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure()).hasMessage(property + " must be true or false; received '" + value + "'");
                    });
            // @formatter:on
        }
    }

    static Stream<Arguments> invalidQueueWriterFlags() {
        // @formatter:off
        return Stream.of("log", "accumulo", "dump")
                .flatMap(backend -> Stream.of("1", "yes", "on", "0", "no", "off", "", " true ", "false ", "invalid")
                        .map(value -> Arguments.of(backend, value)));
        // @formatter:on
    }

    @Test
    void missingQueueWriterFlagsRemainDisabledInCombinedRole() {
        // @formatter:off
        roles
                .withUserConfiguration(ApiDependencies.class)
                .withPropertyValues(
                        "spring.profiles.active=combined",
                        "annotation.deployment.overlays.combined=true")
                .run(context -> {
                    assertThat(context)
                            .hasNotFailed()
                            .hasBean("annotationSource")
                            .doesNotHaveBean("logAnnotationSink")
                            .doesNotHaveBean("accumuloAnnotationSink")
                            .doesNotHaveBean("dumpAnnotationSink");
                    assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.definition"))
                            .isEqualTo("annotationSource;busConsumer");
                });
        // @formatter:on
    }

    @Configuration(proxyBeanMethods = false)
    @Import({AnnotationRoleConfiguration.class, AnnotationServiceConfig.class, AnnotationJsonConfiguration.class, AnnotationSerializerConfiguration.class,
            AnnotationControllerV1.class, LookupConfig.class, HttpClientConfig.class, LookupRequest.class, LookupService.class,
            LogAnnotationWriterProperties.class, AccumuloAnnotationWriterConfig.class, FileAnnotationWriterConfig.class, DumpAnnotationWriterConfig.class})
    static class RoleComponents {}

    @Configuration(proxyBeanMethods = false)
    static class ApiDependencies {
        @Bean
        AccumuloConnectionFactory connectionFactory() {
            return Mockito.mock(AccumuloConnectionFactory.class);
        }

        @Bean
        SSLContext sslContext() throws Exception {
            return SSLContext.getDefault();
        }
    }
}

package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.cloud.bus.BusEnvironmentPostProcessor;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;

import datawave.microservice.annotation.service.config.AnnotationProperties;

class AnnotationRoleProfileTest {
    @Test
    void apiProfileLoadsPublisherSettingsWithoutConsumers() throws IOException {
        StandardEnvironment environment = configuration("api");
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(AnnotationRole.from(environment)).isEqualTo(AnnotationRole.API);
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("annotationSource;busConsumer");
        assertThat(environment.getProperty("annotation.writers.file.enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("annotation.annotation-ack-timeout-millis", Integer.class)).isEqualTo(500);
    }

    @Test
    void writerProfileLoadsConsumerSettingsWithoutPublisher() throws IOException {
        StandardEnvironment environment = configuration("writer");
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(AnnotationRole.from(environment)).isEqualTo(AnnotationRole.WRITER);
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("accumuloAnnotationSink;busConsumer");
        assertThat(environment.getProperty("annotation.writers.accumulo.concurrency", Integer.class)).isEqualTo(1);
        assertThat(Binder.get(environment).bind("annotation", AnnotationProperties.class).get().isEnableInternalIdLookup()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"api", "writer", "combined"})
    void roleProfilesPreserveSharedTuningAndBackendSelection(String profile) throws IOException {
        // @formatter:off
        StandardEnvironment environment = configuration(Map.of(
                "annotation.annotation-ack-timeout-millis", 2500,
                "annotation.retry.max-attempts", 3,
                "annotation.retry.backoff-interval-millis", 1700,
                "annotation.retry.fail-timeout-millis", 19000,
                "annotation.writers.accumulo.enabled", false,
                "annotation.writers.log.enabled", true,
                "annotation.writers.dump.enabled", true,
                "spring.cloud.stream.bindings.accumuloAnnotationSink-in-0.consumer.concurrency", 8), profile);
        // @formatter:on
        AnnotationRoleConfiguration.configureFunctions(environment);

        AnnotationProperties properties = Binder.get(environment).bind("annotation", AnnotationProperties.class).get();
        assertThat(properties.getAnnotationAckTimeoutMillis()).isEqualTo(2500);
        assertThat(properties.getRetry().getMaxAttempts()).isEqualTo(3);
        assertThat(properties.getRetry().getBackoffIntervalMillis()).isEqualTo(1700);
        assertThat(properties.getRetry().getFailTimeoutMillis()).isEqualTo(19000);
        assertThat(environment.getProperty("annotation.writers.accumulo.enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("annotation.writers.log.enabled", Boolean.class)).isTrue();
        assertThat(environment.getProperty("annotation.writers.dump.enabled", Boolean.class)).isTrue();
        assertThat(environment.getProperty("spring.cloud.stream.bindings.accumuloAnnotationSink-in-0.consumer.concurrency", Integer.class)).isEqualTo(8);
        assertThat(environment.getProperty("annotation.writers.accumulo.concurrency", Integer.class)).isEqualTo(8);
        String expected = "api".equals(profile) ? "annotationSource;busConsumer"
                        : "writer".equals(profile) ? "logAnnotationSink;dumpAnnotationSink;busConsumer"
                                        : "annotationSource;logAnnotationSink;dumpAnnotationSink;busConsumer";
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"api", "writer", "combined"})
    void onlyTheApiOverlayChangesAcceptancePolicyDefaults(String profile) throws IOException {
        StandardEnvironment environment = configuration(Map.of("annotation.annotation-ack-enabled", false, "annotation.writers.file.enabled", true), profile);

        AnnotationProperties properties = Binder.get(environment).bind("annotation", AnnotationProperties.class).get();
        boolean api = "api".equals(profile);
        assertThat(properties.isAnnotationAckEnabled()).isEqualTo(api);
        assertThat(environment.getProperty("annotation.writers.file.enabled", Boolean.class)).isEqualTo(!api);
    }

    @Test
    void laterDeploymentProfileCanEnableApiFileFallback() throws IOException {
        StandardEnvironment environment = configuration("api", "deployment-fallback");
        environment.getPropertySources().addFirst(new MapPropertySource("deployment-fallback", Map.of("annotation.writers.file.enabled", true)));
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(environment.getProperty("annotation.writers.file.enabled", Boolean.class)).isTrue();
        assertThat(Binder.get(environment).bind("annotation", AnnotationProperties.class).get().isAnnotationAckEnabled()).isTrue();
        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("annotationSource;busConsumer");
    }

    @Test
    void laterDeploymentProfileCanSelectAFileOnlyWriter() throws IOException {
        StandardEnvironment environment = configuration("writer", "deployment-files");
        environment.getPropertySources().addFirst(new MapPropertySource("deployment-files", Map.of("annotation.writers.accumulo.enabled", false,
                        "annotation.writers.dump.enabled", true, "spring.cloud.stream.bindings.dumpAnnotationSink-in-0.consumer.concurrency", 3)));
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(environment.getProperty("spring.cloud.function.definition")).isEqualTo("dumpAnnotationSink;busConsumer");
        assertThat(environment.getProperty("spring.cloud.stream.bindings.dumpAnnotationSink-in-0.consumer.concurrency", Integer.class)).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"api,writer", "api,combined", "writer,combined", "api,writer,combined"})
    void multipleRoleProfilesFail(String profiles) throws IOException {
        StandardEnvironment environment = configuration(profiles.split(","));
        // @formatter:off
        assertThatThrownBy(() -> AnnotationRole.from(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Activate exactly one of combined, api, writer; selected ");
        // @formatter:on
    }

    @ParameterizedTest
    @ValueSource(strings = {"api", "writer", "combined"})
    void missingRoleOverlayFailsBeforeFunctionsAreConfigured(String profile) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles(profile);

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRoleConfiguration.configureFunctions(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires its matching Config Server overlay annotation-" + profile + ".yml");
        // @formatter:on
        assertThat(environment.getPropertySources().contains("annotationRoleFunctions")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "k8s,remoteauth", "annotation-write", "annotation-api", "annotation-writer", "annotation-combined"})
    void missingRoleProfileFailsEvenWithEnvironmentProfiles(String profiles) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles(profiles.isEmpty() ? new String[0] : profiles.split(","));

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRole.from(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Activate exactly one of combined, api, writer; selected ");
        // @formatter:on
    }

    @Test
    void anotherRolesOverlayCannotSatisfyTheSelectedRole() throws IOException {
        StandardEnvironment environment = configuration("api");
        environment.setActiveProfiles("writer");

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRoleConfiguration.configureFunctions(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("annotation-writer.yml");
        // @formatter:on
    }

    @ParameterizedTest
    @ValueSource(strings = {"api", "writer", "combined"})
    void oldOverlayMarkersCannotSatisfyTheRenamedRole(String profile) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles(profile);
        environment.getPropertySources().addFirst(new MapPropertySource("old-overlay", Map.of("annotation.deployment.overlays.annotation-" + profile, true)));

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRoleConfiguration.configureFunctions(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing annotation.deployment.overlays." + profile);
        // @formatter:on
        assertThat(environment.getPropertySources().contains("annotationRoleFunctions")).isFalse();
    }

    @Test
    void defaultProfilesCannotSupplyAnImplicitRole() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setDefaultProfiles("combined");

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRole.from(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Activate exactly one");
        // @formatter:on
    }

    @Test
    void combinedProfileRetainsSharedSettingsAndBothResponsibilities() throws IOException {
        StandardEnvironment environment = configuration("combined", "dev");
        AnnotationRoleConfiguration.configureFunctions(environment);

        assertThat(AnnotationRole.from(environment)).isEqualTo(AnnotationRole.COMBINED);
        assertThat(environment.getProperty("spring.cloud.function.definition")).contains("annotationSource", "accumuloAnnotationSink");
    }

    @ParameterizedTest
    @MethodSource("functionOverrides")
    void explicitFunctionListsAreRejected(String profile, String property, String definition) throws IOException {
        StandardEnvironment environment = configuration(profile);
        environment.getPropertySources().addFirst(new MapPropertySource("unsupported-functions", Map.of(property, definition)));
        ConfigurationPropertySources.attach(environment);
        new BusEnvironmentPostProcessor().postProcessEnvironment(environment, new SpringApplication(AnnotationRoleConfiguration.class));

        // @formatter:off
        assertThatThrownBy(() -> AnnotationRoleConfiguration.configureFunctions(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(property + " is managed by the annotation service")
                .hasMessageContaining("remove the override");
        // @formatter:on
        assertThat(environment.getPropertySources().contains("annotationRoleFunctions")).isFalse();
    }

    private static Stream<Arguments> functionOverrides() {
        // @formatter:off
        return Stream.of("api", "writer", "combined")
                .flatMap(profile -> Stream.of("spring.cloud.function.definition", "spring.cloud.stream.function.definition")
                        .flatMap(property -> Stream.of(
                                "",
                                "annotationSource",
                                "identity|logAnnotationSink",
                                "custom;other",
                                "busConsumer",
                                "annotationSource;busConsumer")
                                .map(definition -> Arguments.of(profile, property, definition))));
        // @formatter:on
    }

    @Test
    void serviceAndDockerRoleOverlaysHaveIdenticalSettings() throws IOException {
        Path service = Path.of(System.getProperty("basedir")).toAbsolutePath();
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        for (String profile : new String[] {"api", "writer", "combined"}) {
            String filename = "annotation-" + profile + ".yml";
            assertThat(loader.load(profile, new FileSystemResource(service.resolve("src/main/config").resolve(filename))).get(0).getSource()).isEqualTo(
                            loader.load(profile, new FileSystemResource(service.resolve("../../../../docker/config").resolve(filename))).get(0).getSource());
        }
    }

    private static StandardEnvironment configuration(String... profiles) throws IOException {
        return configuration(Map.of(), profiles);
    }

    private static StandardEnvironment configuration(Map<String,Object> sharedOverrides, String... profiles) throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles(profiles);
        Path config = Path.of(System.getProperty("basedir"), "src/main/config");
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        environment.getPropertySources().addLast(loader
                        .load("packaged", new FileSystemResource(Path.of(System.getProperty("basedir"), "src/main/resources/config/bootstrap.yml"))).get(0));
        environment.getPropertySources().addFirst(loader.load("shared", new FileSystemResource(config.resolve("annotation.yml"))).get(0));
        environment.getPropertySources().addFirst(new MapPropertySource("shared-tuning", sharedOverrides));
        for (String profile : profiles) {
            Path overlay = config.resolve("annotation-" + profile + ".yml");
            if (java.nio.file.Files.exists(overlay)) {
                environment.getPropertySources().addFirst(loader.load(profile, new FileSystemResource(overlay)).get(0));
            }
        }
        return environment;
    }
}

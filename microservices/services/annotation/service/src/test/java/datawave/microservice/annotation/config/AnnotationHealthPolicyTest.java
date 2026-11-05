package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.actuate.autoconfigure.availability.AvailabilityProbesAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.endpoint.ApiVersion;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.WebServerNamespace;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthContributorRegistry;
import org.springframework.boot.actuate.health.HealthEndpointGroups;
import org.springframework.boot.actuate.health.HealthEndpointWebExtension;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.availability.ApplicationAvailabilityAutoConfiguration;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.io.FileSystemResource;

/** Verifies production YAML with Boot's real health aggregation, HTTP mapping, and availability groups. */
class AnnotationHealthPolicyTest {
    @ParameterizedTest
    @ValueSource(strings = {"api", "writer", "combined"})
    void requiredRouteHealthControlsReadinessWithoutChangingLiveness(String role) {
        AtomicReference<Status> route = new AtomicReference<>(new Status("RABBITMQ_UNHEALTHY"));
        AtomicReference<Status> backend = new AtomicReference<>(Status.UP);
        // @formatter:off
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ApplicationAvailabilityAutoConfiguration.class,
                        HealthEndpointAutoConfiguration.class,
                        AvailabilityProbesAutoConfiguration.class))
                .withBean("healthChecker", HealthIndicator.class, () -> () -> Health.status(route.get()).build())
                .withBean("backend", HealthIndicator.class, () -> () -> Health.status(backend.get()).build())
                .withPropertyValues(productionProperties())
                .withPropertyValues("spring.profiles.active=" + role)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getEnvironment().getProperty("management.endpoint.health.probes.enabled", Boolean.class)).isTrue();
                    assertThat(context)
                            .hasBean("livenessStateHealthIndicator")
                            .hasBean("readinessStateHealthIndicator");
                    assertThat(context.getBean(HealthEndpointGroups.class).getNames()).contains("readiness", "liveness");
                    AvailabilityChangeEvent.publish(context, LivenessState.CORRECT);
                    AvailabilityChangeEvent.publish(context, ReadinessState.ACCEPTING_TRAFFIC);
                    HealthEndpointWebExtension web = new HealthEndpointWebExtension(
                            context.getBean(HealthContributorRegistry.class),
                            context.getBean(HealthEndpointGroups.class));
                    backend.set(Status.UNKNOWN);
                    assertHealth(web, null, "RABBITMQ_UNHEALTHY", 503);
                    assertHealth(web, "readiness", "RABBITMQ_UNHEALTHY", 503);
                    assertHealth(web, "liveness", "UP", 200);

                    // Topology health does not depend on consumer count; RabbitHealthCheckerTest covers that distinction.
                    route.set(Status.UP);
                    assertHealth(web, null, "UP", 200);
                    assertHealth(web, "readiness", "UP", 200);
                    backend.set(Status.UP);

                    backend.set(Status.DOWN);
                    assertHealth(web, null, "DOWN", 503);
                    assertHealth(web, "readiness", "DOWN", 503);
                    assertHealth(web, "liveness", "UP", 200);
                    backend.set(Status.OUT_OF_SERVICE);
                    assertHealth(web, "readiness", "OUT_OF_SERVICE", 503);
                    backend.set(Status.UP);

                    AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
                    assertHealth(web, "readiness", "OUT_OF_SERVICE", 503);
                    assertHealth(web, "liveness", "UP", 200);
                    AvailabilityChangeEvent.publish(context, ReadinessState.ACCEPTING_TRAFFIC);
                    route.set(new Status("RABBITMQ_UNHEALTHY"));
                    assertHealth(web, "readiness", "RABBITMQ_UNHEALTHY", 503);
                    assertHealth(web, "liveness", "UP", 200);
                });
        // @formatter:on
    }

    private static String[] productionProperties() {
        Path file = Path.of(System.getProperty("basedir"), "src/main/resources/config/bootstrap.yml");
        try {
            EnumerablePropertySource<?> source = (EnumerablePropertySource<?>) new YamlPropertySourceLoader()
                            .load("production-bootstrap", new FileSystemResource(file)).get(0);
            // @formatter:off
            return Stream.of(source.getPropertyNames())
                    .map(name -> name + "=" + source.getProperty(name))
                    .toArray(String[]::new);
            // @formatter:on
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load annotation bootstrap configuration", e);
        }
    }

    private static void assertHealth(HealthEndpointWebExtension web, String group, String status, int httpStatus) {
        WebEndpointResponse<HealthComponent> response = group == null ? web.health(ApiVersion.V3, WebServerNamespace.MANAGEMENT, SecurityContext.NONE)
                        : web.health(ApiVersion.V3, WebServerNamespace.MANAGEMENT, SecurityContext.NONE, group);
        assertThat(response.getStatus()).isEqualTo(httpStatus);
        assertThat(response.getBody().getStatus().getCode()).isEqualTo(status);
    }
}

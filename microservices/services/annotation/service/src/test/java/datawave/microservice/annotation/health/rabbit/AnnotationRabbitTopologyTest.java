package datawave.microservice.annotation.health.rabbit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.microservice.annotation.health.rabbit.config.RabbitHealthProperties;

class AnnotationRabbitTopologyTest {
    private static final String MANAGEMENT_URL = "http://rabbitmq:15672/api/";
    private final ObjectMapper mapper = new ObjectMapper();
    private RabbitHealthChecker checker;
    private MockRestServiceServer server;

    @ParameterizedTest
    @ValueSource(strings = {"annotation-api", "annotation-writer", "combined"})
    void logQueueAloneDoesNotSatisfyTheRequiredPersistenceRoute(String profile) throws IOException {
        StandardEnvironment environment = configuration(profile);
        assertThat(environment.getProperty("annotation.writers.accumulo.enabled", Boolean.class)).isTrue();
        assertThat(environment.getProperty("annotation.writers.log.enabled", Boolean.class)).isFalse();
        initialize(environment);

        expectTopology("log", true);
        checker.runHealthCheck();

        assertThat(checker.isHealthy()).isFalse();
        server.verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {"annotation-api", "annotation-writer", "combined"})
    void requiredDurableRouteIsHealthyWithZeroConsumers(String profile) throws IOException {
        initialize(configuration(profile));
        expectTopology("accumulo", true);

        checker.runHealthCheck();

        assertThat(checker.isHealthy()).isTrue();
        server.verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {"annotation-api", "annotation-writer", "combined"})
    void recoveryProvisionsThePersistenceRouteWithoutCreatingLogQueues(String profile) throws IOException {
        initialize(configuration(profile));
        expectTopology(null, false);
        checker.runHealthCheck();
        assertThat(checker.isHealthy()).isFalse();
        server.verify();
        server.reset();

        server.expect(requestTo(MANAGEMENT_URL + "queues/%2F/annotation.accumulo")).andExpect(method(HttpMethod.PUT))
                        .andExpect(content().json(mapper.writeValueAsString(Map.of("durable", true, "auto_delete", false, "arguments",
                                        Map.of("x-dead-letter-exchange", "DLX", "x-dead-letter-routing-key", "annotation.accumulo"))), false))
                        .andRespond(withSuccess());
        server.expect(requestTo(MANAGEMENT_URL + "queues/%2F/annotation.accumulo.dlq")).andExpect(method(HttpMethod.PUT))
                        .andExpect(content().json("{\"durable\":true,\"auto_delete\":false}", false)).andRespond(withSuccess());
        server.expect(requestTo(MANAGEMENT_URL + "bindings/%2F/e/annotation/q/annotation.accumulo")).andExpect(method(HttpMethod.POST))
                        .andExpect(content().json("{\"routing_key\":\"#\"}", false)).andRespond(withSuccess());
        server.expect(requestTo(MANAGEMENT_URL + "bindings/%2F/e/DLX/q/annotation.accumulo.dlq")).andExpect(method(HttpMethod.POST))
                        .andExpect(content().json("{\"routing_key\":\"annotation.accumulo\"}", false)).andRespond(withSuccess());

        checker.recover();
        server.verify();
        server.reset();
        expectTopology("accumulo", true);
        checker.runHealthCheck();
        assertThat(checker.isHealthy()).isTrue();
        server.verify();
    }

    @Test
    void queueWithoutItsPersistenceBindingIsUnhealthy() throws IOException {
        initialize(configuration("annotation-api"));
        expectTopology("accumulo", false);

        checker.runHealthCheck();

        assertThat(checker.isHealthy()).isFalse();
        server.verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {"log", "dump"})
    void deploymentTopologyOverrideChecksTheSelectedBackend(String backend) throws IOException {
        StandardEnvironment environment = configuration("annotation-writer");
        // A deployment profile supplies complete lists, including its own DLQ route.
        Map<String,Object> deployment = Map.of("annotation",
                        Map.of("writers", Map.of("accumulo", Map.of("enabled", false), backend, Map.of("enabled", true)), "health",
                                        Map.of("rabbit", Map.of(
                                                        "queues", List.of(
                                                                        Map.of("name", "annotation." + backend, "durable", true, "exclusive", false,
                                                                                        "autoDelete", false, "arguments",
                                                                                        Map.of("x-dead-letter-exchange", "DLX", "x-dead-letter-routing-key",
                                                                                                        "annotation." + backend)),
                                                                        Map.of("name", "annotation." + backend + ".dlq", "durable", true, "exclusive", false,
                                                                                        "autoDelete", false)),
                                                        "bindings",
                                                        List.of(Map.of("source", "annotation", "destination", "annotation." + backend, "destinationType",
                                                                        "queue", "routingKey", "#"),
                                                                        Map.of("source", "DLX", "destination", "annotation." + backend + ".dlq",
                                                                                        "destinationType", "queue", "routingKey", "annotation." + backend))))));
        environment.getPropertySources().addFirst(
                        new YamlPropertySourceLoader().load("deployment-backend", new ByteArrayResource(mapper.writeValueAsBytes(deployment))).get(0));
        assertThat(environment.getProperty("annotation.writers." + backend + ".enabled", Boolean.class)).isTrue();
        initialize(environment);

        expectTopology("accumulo", true);
        checker.runHealthCheck();
        assertThat(checker.isHealthy()).isFalse();
        server.verify();
        server.reset();

        expectTopology(backend, true);
        checker.runHealthCheck();
        assertThat(checker.isHealthy()).isTrue();
        server.verify();
    }

    private void initialize(StandardEnvironment environment) {
        RabbitHealthProperties properties = Binder.get(environment).bind("annotation.health.rabbit", Bindable.of(RabbitHealthProperties.class)).get();
        checker = new RabbitHealthChecker(properties, "localhost", "guest", "guest");
        RestTemplate restTemplate = (RestTemplate) new DirectFieldAccessor(new DirectFieldAccessor(checker).getPropertyValue("rabbitClient"))
                        .getPropertyValue("rt");
        server = MockRestServiceServer.createServer(restTemplate);
    }

    private void expectTopology(String group, boolean bindingsPresent) throws IOException {
        expect("nodes/", List.of(Map.of("name", "rabbit@localhost", "running", true, "type", "disc")));
        expect("exchanges/", List.of(exchange("annotation", "topic"), exchange("DLX", "direct")));
        expect("queues/", group == null ? List.of() : List.of(queue("annotation." + group, true), queue("annotation." + group + ".dlq", false)));
        expect("bindings/", !bindingsPresent ? List.of()
                        : List.of(binding("annotation", "annotation." + group, "#"), binding("DLX", "annotation." + group + ".dlq", "annotation." + group)));
    }

    private void expect(String endpoint, Object response) throws IOException {
        server.expect(requestTo(MANAGEMENT_URL + endpoint)).andRespond(withSuccess(mapper.writeValueAsString(response), MediaType.APPLICATION_JSON));
    }

    private static Map<String,Object> exchange(String name, String type) {
        return Map.of("name", name, "type", type, "durable", true, "auto_delete", false, "internal", false);
    }

    private static Map<String,Object> queue(String name, boolean deadLettering) {
        return Map.of("name", name, "durable", true, "exclusive", false, "auto_delete", false, "consumers", 0, "arguments",
                        deadLettering ? Map.of("x-dead-letter-exchange", "DLX", "x-dead-letter-routing-key", name) : Map.of());
    }

    private static Map<String,Object> binding(String source, String destination, String routingKey) {
        return Map.of("source", source, "destination", destination, "destination_type", "queue", "routing_key", routingKey, "arguments", Map.of());
    }

    private static StandardEnvironment configuration(String profile) throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        Path config = Path.of(System.getProperty("basedir"), "src/main/config");
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        environment.getPropertySources().addFirst(loader.load("shared", new FileSystemResource(config.resolve("annotation.yml"))).get(0));
        if (!profile.equals("combined")) {
            environment.setActiveProfiles(profile);
            environment.getPropertySources().addFirst(loader.load(profile, new FileSystemResource(config.resolve("annotation-" + profile + ".yml"))).get(0));
        }
        return environment;
    }
}

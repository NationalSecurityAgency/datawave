package datawave.microservice.annotation.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.FileSystemResource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import datawave.autoconfigure.DatawaveCacheAutoConfiguration;
import datawave.microservice.authorization.federation.config.FederatedAuthorizationServiceRegistrarConfiguration;
import datawave.microservice.cached.HazelcastCacheClientHealthIndicator;
import datawave.microservice.cached.HazelcastDefaultClientAutoConfiguration;
import datawave.microservice.cached.HazelcastDiscoveryClientAutoConfiguration;
import datawave.microservice.cached.HazelcastK8sClientAutoConfiguration;
import datawave.microservice.config.marking.MarkingConfig;

/** Exercises ordinary role YAML and real cache auto-configuration, without test-profile cache overrides or mocked Hazelcast beans. */
class AnnotationCachePolicyTest {
    @ParameterizedTest
    @MethodSource("ordinaryProfiles")
    void allRolesUseLocalCachesWithoutARemoteClient(String layout, String role, String discovery) {
        Path service = Path.of(System.getProperty("basedir")).toAbsolutePath();
        Path config = "service".equals(layout) ? service.resolve("src/main/config") : service.resolve("../../../../docker/config");
        new ApplicationContextRunner()
                        .withConfiguration(AutoConfigurations.of(HazelcastDefaultClientAutoConfiguration.class, HazelcastDiscoveryClientAutoConfiguration.class,
                                        HazelcastK8sClientAutoConfiguration.class, HazelcastAutoConfiguration.class, DatawaveCacheAutoConfiguration.class,
                                        CacheAutoConfiguration.class))
                        .withUserConfiguration(MarkingConfig.class, FederatedAuthorizationServiceRegistrarConfiguration.class, HealthComponents.class)
                        .withInitializer(context -> {
                            context.getEnvironment().setActiveProfiles("annotation-" + role, discovery);
                            YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
                            try {
                                context.getEnvironment().getPropertySources()
                                                .addFirst(loader.load("shared", new FileSystemResource(config.resolve("annotation.yml"))).get(0));
                                context.getEnvironment().getPropertySources().addFirst(
                                                loader.load("role", new FileSystemResource(config.resolve("annotation-annotation-" + role + ".yml"))).get(0));
                            } catch (IOException e) {
                                throw new IllegalStateException("Cannot load ordinary annotation configuration", e);
                            }
                        }).run(context -> {
                            assertThat(context).hasNotFailed().doesNotHaveBean(ClientConfig.class).doesNotHaveBean(HazelcastInstance.class)
                                            .doesNotHaveBean(HazelcastCacheClientHealthIndicator.class);
                            assertThat(context.getBean("cacheManager")).isInstanceOf(CaffeineCacheManager.class);
                            assertThat(context.getBean("markings-cache-manager")).isInstanceOf(CaffeineCacheManager.class);
                            assertThat(context.getBean("remoteOperationsCacheManager")).isInstanceOf(CaffeineCacheManager.class);
                            for (String manager : new String[] {"cacheManager", "markings-cache-manager", "remoteOperationsCacheManager"}) {
                                CaffeineCacheManager caches = context.getBean(manager, CaffeineCacheManager.class);
                                String name = "remoteOperationsCacheManager".equals(manager) ? "getRemoteUser" : "annotation-cache-probe";
                                Cache cache = caches.getCache(name);
                                assertThat(cache).isNotNull();
                                cache.put("key", "value");
                                assertThat(cache.get("key", String.class)).isEqualTo("value");
                                cache.evict("key");
                                assertThat(cache.get("key")).isNull();
                            }
                        });
    }

    private static Stream<Arguments> ordinaryProfiles() {
        return Stream.of("service", "docker").flatMap(layout -> Stream.of("api", "writer", "combined")
                        .flatMap(role -> Stream.of("default", "k8s", "consul").map(discovery -> Arguments.of(layout, role, discovery))));
    }

    @TestConfiguration(proxyBeanMethods = false)
    @Import(HazelcastCacheClientHealthIndicator.class)
    static class HealthComponents {}
}

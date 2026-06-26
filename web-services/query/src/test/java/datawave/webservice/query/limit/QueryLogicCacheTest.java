package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERY_LOGICS_ROOT_PATH;
import static datawave.webservice.query.limit.QueryLimiterUtils.ZOOKEEPER_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryLogicCacheTest {

    private TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify that when there are no preexisting nodes under the path {@value QueryLimiterUtils#QUERY_LOGICS_ROOT_PATH} in Zookeeper, no query logics will be
     * present in the initial cache.
     */
    @Test
    void testNoInitialQueryLogicsInZookeeper() throws Exception {
        try (CuratorFramework client = getClient()) {
            QueryLogicCache cache = new QueryLogicCache(client);

            // Wait until the cache is initialized.
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            // Assert that initially, there are no cached query logics.
            assertThat(cache.getQueryLogics()).isEmpty();
        }
    }

    /**
     * Verify that when there are preexisting nodes under the path {@value QueryLimiterUtils#QUERY_LOGICS_ROOT_PATH} in Zookeeper, those query logics will be
     * present in the initial cache.
     */
    @Test
    void testInitialQueryLogicsInZookeeper() throws Exception {
        try (CuratorFramework client = getClient()) {
            // Create some initial pre-existing query logics.
            client.createContainers(QUERY_LOGICS_ROOT_PATH);
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/ShardQueryLogic");
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/TLDQueryLogic");
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/EventQueryLogic");

            QueryLogicCache cache = new QueryLogicCache(client);

            // Wait until the cache is initialized.
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            // Verify the initial set of query logics.
            assertThat(cache.getQueryLogics()).containsExactlyInAnyOrder("ShardQueryLogic", "TLDQueryLogic", "EventQueryLogic");
        }
    }

    /**
     * Verify that when a new query logic is added under the path {@value QueryLimiterUtils#QUERY_LOGICS_ROOT_PATH} via {@link ActiveQueryTracker}, that the
     * query logic is added to the cache, and its creation is supplied to any registered listeners.
     */
    @Test
    void testCreationOfQueryLogic() throws Exception {
        try (CuratorFramework client = getClient()) {
            ActiveQueryTracker tracker = new ActiveQueryTracker(client);
            QueryLogicCache cache = new QueryLogicCache(client);

            // Wait until the cache is initialized.
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            // Add two listeners that will update the contents of two separate sets.
            Set<String> listeningSetA = new HashSet<>();
            cache.addListener(createListener(listeningSetA));
            Set<String> listeningSetB = new HashSet<>();
            cache.addListener(createListener(listeningSetB));

            // Assert that initially, there are no cached query logics.
            assertThat(cache.getQueryLogics()).isEmpty();

            String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
            String system = "SERVER-01";

            // Track several queries with different query logics, some repeated.
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EdgeQueryLogic");

            // Wait a short period to ensure the paths are created and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Assert that the correct query logics are cached.
            assertThat(cache.getQueryLogics()).containsExactlyInAnyOrder("ShardQueryLogic", "EventQueryLogic", "TLDQueryLogic", "EdgeQueryLogic");

            // Assert that the listening sets were updated with the query logics.
            assertThat(listeningSetA).containsExactlyInAnyOrder("ShardQueryLogic", "EventQueryLogic", "TLDQueryLogic", "EdgeQueryLogic");
            assertThat(listeningSetB).containsExactlyInAnyOrder("ShardQueryLogic", "EventQueryLogic", "TLDQueryLogic", "EdgeQueryLogic");
        }
    }

    /**
     * Verify that when an existing query logic under the path {@value QueryLimiterUtils#QUERY_LOGICS_ROOT_PATH} is deleted, that the query logic is removed
     * from the cache, and its deletion is supplied to any registered listeners.
     */
    @Test
    void testDeletionOfQueryLogic() throws Exception {
        try (CuratorFramework client = getClient()) {
            // Create some initial pre-existing query logics.
            client.createContainers(QUERY_LOGICS_ROOT_PATH);
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/ShardQueryLogic");
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/TLDQueryLogic");
            client.create().forPath(QUERY_LOGICS_ROOT_PATH + "/EventQueryLogic");

            QueryLogicCache cache = new QueryLogicCache(client);

            // Wait until the cache is initialized.
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            // Assert that the correct query logics are cached.
            assertThat(cache.getQueryLogics()).containsExactlyInAnyOrder("ShardQueryLogic", "EventQueryLogic", "TLDQueryLogic");

            // Add two listeners that will update the contents of two separate sets that reflect the cached query logics.
            Set<String> listeningSetA = new HashSet<>(cache.getQueryLogics());
            cache.addListener(createListener(listeningSetA));
            Set<String> listeningSetB = new HashSet<>(cache.getQueryLogics());
            cache.addListener(createListener(listeningSetB));

            // Delete some of the query logic nodes.
            client.delete().forPath(QUERY_LOGICS_ROOT_PATH + "/ShardQueryLogic");
            client.delete().forPath(QUERY_LOGICS_ROOT_PATH + "/TLDQueryLogic");

            // Wait a short period to ensure the paths are deleted and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Assert that the correct query logics are cached.
            assertThat(cache.getQueryLogics()).containsExactlyInAnyOrder("EventQueryLogic");

            // Assert that the listening sets were also updated.
            assertThat(listeningSetA).containsExactlyInAnyOrder("EventQueryLogic");
            assertThat(listeningSetB).containsExactlyInAnyOrder("EventQueryLogic");
        }
    }

    private CuratorFramework getClient() throws InterruptedException {
        // @formatter:off
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .namespace(ZOOKEEPER_NAMESPACE)
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
        client.start();
        client.blockUntilConnected(5, TimeUnit.SECONDS);
        return client;
    }

    private QueryLogicsUpdateListener createListener(Set<String> set) {
        return new QueryLogicsUpdateListener() {

            @Override
            public void forCreate(String queryLogic) {
                set.add(queryLogic);
            }

            @Override
            public void forDelete(String queryLogic) {
                set.remove(queryLogic);
            }
        };
    }

}

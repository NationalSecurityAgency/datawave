package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.ZOOKEEPER_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
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

class QueryCountsCacheTest {

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
     * Verify that when a {@link QueryCountsCache} is first initialized, it will return 0 for any mappings not present.
     */
    @Test
    void testInitialCounts() throws InterruptedException {
        try (CuratorFramework client = getClient()) {
            QueryCountsCache cache = new QueryCountsCache(client, getLimitProvider());
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            assertThat(cache.getTotalUserQueries("cn=user")).isEqualTo(0);
            assertThat(cache.getTotalUserQueries("cn=user", "ShardQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "ShardQueryLogic")).isEqualTo(0);
        }
    }

    /**
     * Verify that as queries are created and updated for users, the underlying caches are updated and reflected in the results of
     * {@link QueryCountsCache#getTotalUserQueries(String)}.
     */
    @Test
    void testGetTotalUserQueries() throws Exception {
        try (CuratorFramework client = getClient()) {
            QueryCountsCache cache = new QueryCountsCache(client, getLimitProvider());
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            ActiveQueryTracker tracker = new ActiveQueryTracker(client);

            // Verify that the total query count for an untracked user is 0.
            assertThat(cache.getTotalUserQueries("cn=user")).isEqualTo(0);

            // Track four queries for the user with a mix of systems and query logics.
            QueryHeartbeat heartbeat1 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat2 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "TLDQueryLogic");
            // Queries on SYSTEM-03 do not count against the user limit, and should not be reflected in the user counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");

            // Track 2 queries for another user.
            QueryHeartbeat heartbeat3 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat4 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-02", "EventQueryLogic");
            // Queries on SYSTEM-03 do not count against the user limit, and should not be reflected in the user counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "EventQueryLogic");

            // Wait a short period to ensure the paths are created and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the total query counts.
            assertThat(cache.getTotalUserQueries("cn=user")).isEqualTo(4);
            assertThat(cache.getTotalUserQueries("cn=other user")).isEqualTo(2);

            // Stop tracking some queries via stopping the heartbeats.
            heartbeat1.stop();
            heartbeat2.stop();

            heartbeat3.stop();
            heartbeat4.stop();

            // Wait a short period to ensure the paths are deleted and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the updated total query counts.
            assertThat(cache.getTotalUserQueries("cn=user")).isEqualTo(2);
            assertThat(cache.getTotalUserQueries("cn=other user")).isEqualTo(0);
        }
    }

    /**
     * Verify that as queries are created and updated for users, the underlying caches are updated and reflected in the results of
     * {@link QueryCountsCache#getTotalUserQueries(String, String)}.
     */
    @Test
    void testGetTotalUserQueriesWithQueryLogic() throws Exception {
        try (CuratorFramework client = getClient()) {
            QueryCountsCache cache = new QueryCountsCache(client, getLimitProvider());
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            ActiveQueryTracker tracker = new ActiveQueryTracker(client);

            // Verify that the total query count for an untracked user is 0.
            assertThat(cache.getTotalUserQueries("cn=user", "ShardQueryLogic")).isEqualTo(0);

            // Track queries for a user with a mix of systems and query logics.
            QueryHeartbeat heartbeat1 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat2 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat3 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat4 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "TLDQueryLogic");
            // Queries on SYSTEM-03 do not count against the user limit, and should not be reflected in the user counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");

            // Track queries for another user.
            QueryHeartbeat heartbeat5 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat6 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat7 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-02", "EventQueryLogic");
            // Queries on SYSTEM-03 do not count against the user limit, and should not be reflected in the user counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "EventQueryLogic");

            // Wait a short period to ensure the paths are created and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the total query counts.
            assertThat(cache.getTotalUserQueries("cn=user", "ShardQueryLogic")).isEqualTo(5);
            assertThat(cache.getTotalUserQueries("cn=user", "EventQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalUserQueries("cn=user", "TLDQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalUserQueries("cn=other user", "ShardQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalUserQueries("cn=other user", "EventQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalUserQueries("cn=other user", "TLDQueryLogic")).isEqualTo(0);

            // Stop tracking some queries via stopping the heartbeats.
            heartbeat1.stop();
            heartbeat2.stop();
            heartbeat3.stop();
            heartbeat4.stop();
            heartbeat5.stop();
            heartbeat6.stop();
            heartbeat7.stop();

            // Wait a short period to ensure the paths are deleted and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the updated total query counts.
            assertThat(cache.getTotalUserQueries("cn=user", "ShardQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalUserQueries("cn=user", "EventQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalUserQueries("cn=user", "TLDQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalUserQueries("cn=other user", "ShardQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalUserQueries("cn=other user", "EventQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalUserQueries("cn=other user", "TLDQueryLogic")).isEqualTo(0);
        }
    }

    /**
     * Verify that as queries are created and updated for systems, the underlying caches are updated and reflected in the results of
     * {@link QueryCountsCache#getTotalSystemQueries(String)}.
     */
    @Test
    void testGetTotalSystemQueries() throws Exception {
        try (CuratorFramework client = getClient()) {
            QueryCountsCache cache = new QueryCountsCache(client, getLimitProvider());
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            ActiveQueryTracker tracker = new ActiveQueryTracker(client);

            // Verify that the total query count for an untracked system is 0.
            assertThat(cache.getTotalSystemQueries("SYSTEM-01")).isEqualTo(0);

            // Track four queries for a system with a mix of users and query logics.
            QueryHeartbeat heartbeat1 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat2 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "TLDQueryLogic");

            // Track 2 queries for another system with a mix of users and query logics.
            QueryHeartbeat heartbeat3 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "ShardQueryLogic");
            QueryHeartbeat heartbeat4 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-02", "EventQueryLogic");

            // Queries on SYSTEM-03 do not count against the user limits, but they should still be reflected in the system counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "EventQueryLogic");

            // Wait a short period to ensure the paths are created and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the total query counts.
            assertThat(cache.getTotalSystemQueries("SYSTEM-01")).isEqualTo(4);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03")).isEqualTo(3);

            // Stop tracking some queries via stopping the heartbeats.
            heartbeat1.stop();
            heartbeat2.stop();

            heartbeat3.stop();
            heartbeat4.stop();

            // Wait a short period to ensure the paths are deleted and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the updated total query counts.
            assertThat(cache.getTotalSystemQueries("SYSTEM-01")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03")).isEqualTo(3);
        }
    }

    /**
     * Verify that as queries are created and updated for systems, the underlying caches are updated and reflected in the results of
     * {@link QueryCountsCache#getTotalSystemQueries(String, String)}.
     */
    @Test
    void testGetTotalSystemQueriesWithQueryLogic() throws Exception {
        try (CuratorFramework client = getClient()) {
            QueryCountsCache cache = new QueryCountsCache(client, getLimitProvider());
            Awaitility.await("Healthy cache").atMost(Duration.ofSeconds(5)).until(cache::isHealthy);

            ActiveQueryTracker tracker = new ActiveQueryTracker(client);

            // Verify that the total query count for an untracked system is 0.
            assertThat(cache.getTotalSystemQueries("cn=user", "ShardQueryLogic")).isEqualTo(0);

            // Track queries for a system with a mix of users and query logics.
            QueryHeartbeat heartbeat1 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat2 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat3 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "ShardQueryLogic");
            QueryHeartbeat heartbeat4 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "EventQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-01", "ShardQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-01", "TLDQueryLogic");

            // Track queries for another system.
            QueryHeartbeat heartbeat5 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "ShardQueryLogic");
            QueryHeartbeat heartbeat6 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-02", "ShardQueryLogic");
            QueryHeartbeat heartbeat7 = tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-02", "EventQueryLogic");

            // Queries on SYSTEM-03 do not count against the user limits, but they should still be reflected in the system counts.
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "TLDQueryLogic");
            tracker.trackQuery(UUID.randomUUID().toString(), "cn=other user", "SYSTEM-03", "EventQueryLogic");

            // Wait a short period to ensure the paths are created and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the total query counts.
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "ShardQueryLogic")).isEqualTo(5);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "EventQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "TLDQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "ShardQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "EventQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "TLDQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03", "TLDQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03", "EventQueryLogic")).isEqualTo(1);

            // Stop tracking some queries via stopping the heartbeats.
            heartbeat1.stop();
            heartbeat2.stop();
            heartbeat3.stop();
            heartbeat4.stop();
            heartbeat5.stop();
            heartbeat6.stop();
            heartbeat7.stop();

            // Wait a short period to ensure the paths are deleted and caches are updated.
            Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).until(() -> true);

            // Verify the updated total query counts.
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "ShardQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "EventQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalSystemQueries("SYSTEM-01", "TLDQueryLogic")).isEqualTo(1);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "ShardQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "EventQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-02", "TLDQueryLogic")).isEqualTo(0);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03", "TLDQueryLogic")).isEqualTo(2);
            assertThat(cache.getTotalSystemQueries("SYSTEM-03", "EventQueryLogic")).isEqualTo(1);
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

    private SystemLimitProvider getLimitProvider() {
        SystemLimitConfiguration configuration = new SystemLimitConfiguration();
        configuration.setSystemPattern("SYSTEM-03");
        configuration.setCountsAgainstUserLimit(false);
        return new SystemLimitProvider(100, 100, Collections.singleton(configuration), new QueryLogicGroupLimitProvider(100, null));
    }
}

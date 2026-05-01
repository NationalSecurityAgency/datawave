package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ActiveQueryTrackerTest {

    TestingServer server;
    ActiveQueryTracker tracker;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        tracker = new ActiveQueryTracker(server.getConnectString(), 120000L);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify the correct nodes are created when tracking a query on a system that counts against user limits.
     */
    @Test
    void testQueryOnSystemThatCountsTowardsUserLimit() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        tracker.trackQuery(queryId, user, system, queryLogic, true);

        CuratorFramework client = getClient();
        client.start();

        // Verify that the nodes were created as expected in Zookeeper.
        assertThat(client.checkExists().forPath("/users/" + user.toLowerCase() + "/" + queryLogic + "/" + queryId)).isNotNull();
        assertThat(client.checkExists().forPath("/systems/" + system + "/" + queryLogic + "/" + queryId)).isNotNull();
        assertThat(client.checkExists().forPath("/distinctQueryLogics/" + queryLogic)).isNotNull();
    }

    /**
     * Verify the correct nodes are created when tracking a query on a system that does not count against user limits.
     */
    @Test
    void testQueryOnSystemThatDoesNotCountsTowardsUserLimit() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        tracker.trackQuery(queryId, user, system, queryLogic, false);

        CuratorFramework client = getClient();
        client.start();

        // Verify that the nodes were created as expected in Zookeeper.
        assertThat(client.checkExists().forPath("/systems/" + system + "/" + queryLogic + "/" + queryId)).isNotNull();
        assertThat(client.checkExists().forPath("/distinctQueryLogics/" + queryLogic)).isNotNull();

        // Verify that the user node was not created.
        assertThat(client.checkExists().forPath("/users/" + user.toLowerCase() + "/" + queryLogic + "/" + queryId)).isNull();
    }

    /**
     * Verify that an exception is thrown if we track an already tracked query.
     */
    @Test
    void testQueryCannotBeRetracked() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        tracker.trackQuery(queryId, user, system, queryLogic, true);

        assertThatExceptionOfType(QueryAlreadyTrackedException.class).isThrownBy(() -> tracker.trackQuery(queryId, user, system, queryLogic, true));
    }

    /**
     * Verify that when a heartbeat is stopped, that the ephemeral nodes are deleted.
     */
    @Test
    void testStoppingHeartbeat() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        QueryHeartbeat heartbeat = tracker.trackQuery(queryId, user, system, queryLogic, true);

        heartbeat.stop();

        CuratorFramework client = getClient();
        client.start();

        // Verify the container paths were not deleted.
        assertThat(client.checkExists().forPath("/users/" + user.toLowerCase() + "/" + queryLogic)).isNotNull();
        assertThat(client.checkExists().forPath("/systems/" + system + "/" + queryLogic)).isNotNull();

        // Verify the distinctQueryLogics node was not affected.
        assertThat(client.checkExists().forPath("/distinctQueryLogics/" + queryLogic)).isNotNull();

        // Verify the query ID nodes were deleted.
        assertThat(client.checkExists().forPath("/users/" + user.toLowerCase() + "/" + queryLogic + "/" + queryId)).isNull();
        assertThat(client.checkExists().forPath("/systems/" + system + "/" + queryLogic + "/" + queryId)).isNull();
    }

    /**
     * Verify the expected behavior of {@link ActiveQueryTracker#getDistinctQueryLogics()}.
     */
    @Test
    void testGetDistinctQueryLogics() throws Exception {
        // Assert that initially, there are no distinct query logics.
        assertThat(tracker.getDistinctQueryLogics()).isEmpty();

        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String system = "SERVER-01";

        // Track several queries with different query logics, some repeated.
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "ShardQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "ShardQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "TLDQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, system, "EdgeQueryLogic", true);

        // Assert that distinct query logics are returned.
        List<String> distinctQueryLogics = tracker.getDistinctQueryLogics();
        assertThat(distinctQueryLogics).containsExactlyInAnyOrder("ShardQueryLogic", "EventQueryLogic", "TLDQueryLogic", "EdgeQueryLogic");
    }

    /**
     * Verify the expected behavior of {@link ActiveQueryTracker#getTotalUserQueriesForQueryLogic(String, String)}.
     */
    @Test
    void testGetTotalUserQueriesForQueryLogic() throws Exception {
        String userA = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String userB = "CN=Test B. User, ou=Example Developers, O=Example Corp, c=US";

        // Assert that if there are no queries tracked at all for the user, 0 is returned.
        assertThat(tracker.getTotalUserQueriesForQueryLogic(userA, "ShardQueryLogic")).isEqualTo(0);

        // Track some queries for the users with various query logics.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "TLDQueryLogic", false); // Should not be counted, system doesn't count towards
                                                                                                      // limit.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "TLDQueryLogic", false); // Should not be counted, system doesn't count towards
                                                                                                      // limit.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-02", "EventQueryLogic", true); // Should not be counted, different user.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-03", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-03", "EventQueryLogic", true); // Should not be counted, different user.
        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-03", "ShardQueryLogic", true); // Should not be counted, different user.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-03", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-04", "EventQueryLogic", false); // Should not be counted, system doesn't count towards
                                                                                                        // limit.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-04", "EventQueryLogic", false); // Should not be counted, system doesn't count towards
                                                                                                        // limit.

        // Assert that if there are some queries tracked for the user, but none for the query logic, 0 is returned.
        assertThat(tracker.getTotalUserQueriesForQueryLogic(userA, "ShardQueryLogic")).isEqualTo(0);

        // Assert that the count for TLDQueryLogic is 0 since all queries tracked for them were on a system that do not count against user limits.
        assertThat(tracker.getTotalUserQueriesForQueryLogic(userA, "TLDQueryLogic")).isEqualTo(0);

        // Assert the count for EventQueryLogic is 5 since 5 queries tracked for them for the user were on systems that count against the user limits.
        assertThat(tracker.getTotalUserQueriesForQueryLogic(userA, "EventQueryLogic")).isEqualTo(5);
    }

    /**
     * Verify the expected behavior of {@link ActiveQueryTracker#getTotalSystemQueriesForQueryLogic(String, String)}.
     */
    @Test
    void testGetTotalSystemQueriesForQueryLogic() throws Exception {
        String userA = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";
        String userB = "CN=Test B. User, ou=Example Developers, O=Example Corp, c=US";

        // Assert that if there are no queries tracked at all for the system, 0 is returned.
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-01", "ShardQueryLogic")).isEqualTo(0);

        // Track some queries for the users with various query logics.
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "EventQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-01", "EventQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "EventQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "EventQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-01", "EventQueryLogic", false);

        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-02", "ShardQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userB, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), userA, "SYSTEM-02", "EventQueryLogic", true);

        // Assert the counts for query logics on SYSTEM-01.
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-01", "ShardQueryLogic")).isEqualTo(0);
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-01", "TLDQueryLogic")).isEqualTo(2);
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-01", "EventQueryLogic")).isEqualTo(5);

        // Assert the counts for query logics on SYSTEM-02.
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-02", "ShardQueryLogic")).isEqualTo(1);
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-02", "TLDQueryLogic")).isEqualTo(0);
        assertThat(tracker.getTotalSystemQueriesForQueryLogic("SYSTEM-02", "EventQueryLogic")).isEqualTo(4);
    }

    /**
     * Verify the expected behavior of {@link ActiveQueryTracker#totalUserQueriesMeetsLimit(String, int, Set, int)}
     */
    @Test
    void testTotalUserQueriesMeetsLimit() throws Exception {
        String user = "CN=Test A. User, ou=Example Developers, O=Example Corp, c=US";

        // Assert that if no queries are tracked for the user, we do not meet the limit.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 10, Set.of(), 0)).isFalse();

        // Track some queries for the user that should not count against the user's limit.
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-01", "TLDQueryLogic", false);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-01", "TLDQueryLogic", false);

        // Assert that the above queries will not have us meet a limit of 3 total allowed queries for the user.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 3, Set.of(), 0)).isFalse();

        // Track some queries for the user that should count against the user's limit.
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-02", "TLDQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-02", "TLDQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-02", "TLDQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-02", "TLDQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-03", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-03", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-03", "EventQueryLogic", true);
        tracker.trackQuery(UUID.randomUUID().toString(), user, "SYSTEM-03", "EventQueryLogic", true);

        // Assert that the above queries will meet a limit of 7 total allowed queries for the user.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 7, Set.of(), 0)).isTrue();

        // If we pass in arguments that indicate we already counted the EventQueryLogic queries, assert that a limit of 7 will be met.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 7, Set.of("EventQueryLogic"), 4)).isTrue();

        // If the limit is 10, assert we do not meet the limit.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 10, Set.of(), 0)).isFalse();

        // If we pass in arguments that indicate we already counted the EventQueryLogic queries, assert that a limit of 10 will not be met.
        assertThat(tracker.totalUserQueriesMeetsLimit(user, 10, Set.of("EventQueryLogic"), 4)).isFalse();
    }

    /**
     * Verify the expected behavior of {@link ActiveQueryTracker#totalSystemQueriesMeetsLimit(String, int, Set, int)}. The assertion cases are identical
     * regardless of whether the system counts towards user limits or not.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTotalSystemQueriesMeetsLimit(boolean systemCountsAgainstUserLimits) throws Exception {
        // Assert that if no queries are tracked for the system, we do not meet the limit.
        assertThat(tracker.totalSystemQueriesMeetsLimit("SYSTEM-01", 10, Set.of(), 0)).isFalse();

        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "TLDQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "TLDQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "TLDQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "TLDQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "EventQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "EventQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "EventQueryLogic", systemCountsAgainstUserLimits);
        tracker.trackQuery(UUID.randomUUID().toString(), "cn=userA", "SYSTEM-01", "EventQueryLogic", systemCountsAgainstUserLimits);

        // Assert that the above queries will meet a limit of 7 total allowed queries for the user.
        assertThat(tracker.totalSystemQueriesMeetsLimit("SYSTEM-01", 7, Set.of(), 0)).isTrue();

        // If we pass in arguments that indicate we already counted the EventQueryLogic queries, assert that a limit of 7 will be met.
        assertThat(tracker.totalSystemQueriesMeetsLimit("SYSTEM-01", 7, Set.of("EventQueryLogic"), 4)).isTrue();

        // If the limit is 10, assert we do not meet the limit.
        assertThat(tracker.totalSystemQueriesMeetsLimit("SYSTEM-01", 10, Set.of(), 0)).isFalse();

        // If we pass in arguments that indicate we already counted the EventQueryLogic queries, assert that a limit of 10 will not be met.
        assertThat(tracker.totalSystemQueriesMeetsLimit("SYSTEM-01", 10, Set.of("EventQueryLogic"), 4)).isFalse();
    }

    private CuratorFramework getClient() {
        // @formatter:off
        return CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .namespace("ActiveQueries")
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
    }
}

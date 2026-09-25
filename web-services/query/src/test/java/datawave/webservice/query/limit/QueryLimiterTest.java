package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for testing the functionality of {@link QueryLimiter}.
 */
class QueryLimiterTest {

    private static final String userA = "cn=testuserA, c=us";
    private static final String userB = "cn=testuserB, c=us";
    private static final String system1 = "SYSTEM-01";
    private static final String system2 = "SYSTEM-02";
    private static final String tldQueryLogic = "TLDQueryLogic";
    private static final String eventQueryLogic = "EventQueryLogic";

    private final Map<String,QueryLimiter> systemToLimiter = new HashMap<>();
    private QueryHeartbeatCache heartbeatCache;
    private QueryLimitConfiguration config;
    private TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        heartbeatCache = new QueryHeartbeatCache();
    }

    @AfterEach
    void tearDown() throws IOException {
        heartbeatCache.shutdown();
        systemToLimiter.clear();
        config = null;
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify {@link QueryLimiter#checkForLimits(String, String, String)} returns {@link QueryLimiterResponse#hasNotMetLimit()} when the configuration for the
     * QueryLimiter is null.
     */
    @Test
    void testCheckForLimitsGivenNullConfig() throws Exception {
        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());
        limiter.setHeartbeatCache(new QueryHeartbeatCache());
        limiter.setup();

        QueryLimiterResponse response = limiter.checkForLimits(userA, system1, tldQueryLogic);
        assertThat(response).isEqualTo(QueryLimiterResponse.hasNotMetLimit());
    }

    /**
     * Verify {@link QueryLimiter#checkForLimits(String, String, String)} returns {@link QueryLimiterResponse#hasNotMetLimit()} when the configured
     * QueryLimitConfiguration has a value of 'false' for 'enabled'.
     */
    @Test
    void testCheckForLimitsWhenDisabled() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(2);

        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());
        limiter.setHeartbeatCache(new QueryHeartbeatCache());
        limiter.setConfiguration(config);
        limiter.setup();

        try (ActiveQueryTracker tracker = new ActiveQueryTracker(server.getConnectString())) {
            tracker.trackQuery(UUID.randomUUID().toString(), userA, system1, tldQueryLogic, true);
            tracker.trackQuery(UUID.randomUUID().toString(), userA, system1, tldQueryLogic, true);

            QueryLimiterResponse response = limiter.checkForLimits(userA, system1, tldQueryLogic);
            assertThat(response).isEqualTo(QueryLimiterResponse.hasNotMetLimit());
        }
    }

    /**
     * Verify that queries are not tracked when the configured QueryLimitConfiguration has a value of 'false' for 'enabled'.
     */
    @Test
    void testCountQueryTowardsLimitsWhenDisabled() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(5);

        QueryHeartbeatCache heartbeatCache = new QueryHeartbeatCache();

        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());

        limiter.setConfiguration(config);
        limiter.setHeartbeatCache(heartbeatCache);
        limiter.setup();
        String queryId = UUID.randomUUID().toString();

        assertThat(limiter.isEnforcingLimits()).isFalse();

        if (limiter.getConfiguration() != null && !limiter.isEnforcingLimits()) {
            limiter.countQueryTowardsLimits(queryId, userA, system1, tldQueryLogic);

            // Verify a heartbeat was not added to the heartbeat cache.
            assertThat(heartbeatCache.get(queryId)).isNull();

            // @formatter:off
            try (CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
                    .namespace("ActiveQueries")
                    .sessionTimeoutMs(60000)
                    .connectionTimeoutMs(60000)
                    .retryPolicy(new RetryNTimes(10, 1000))
                    .build()) {
                client.start();
                // Verify that the nodes for tracking the active queries were never created.
                assertThat(client.checkExists().forPath("/users/" + userA.toLowerCase() + "/" + tldQueryLogic + "/" + queryId)).isNull();
                assertThat(client.checkExists().forPath("/systems/" + system1 + "/" + tldQueryLogic + "/" + queryId)).isNull();
            }
            // @formatter:on
        }
    }

    /**
     * Verify that queries are not tracked when the configuration for the QueryLimiter is null.
     */
    @Test
    void testCountQueryTowardsLimitsWhenConfigIsNull() throws Exception { // resolved
        QueryHeartbeatCache heartbeatCache = new QueryHeartbeatCache();

        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());
        limiter.setHeartbeatCache(heartbeatCache);
        limiter.setup();
        String queryId = UUID.randomUUID().toString();

        if (limiter.getConfiguration() == null) {
            limiter.countQueryTowardsLimits(queryId, userA, system1, tldQueryLogic);

            // Verify a heartbeat was not added to the heartbeat cache.
            assertThat(heartbeatCache.get(queryId)).isNull();

            // @formatter:off
            try (CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
                    .namespace("ActiveQueries")
                    .sessionTimeoutMs(60000)
                    .connectionTimeoutMs(60000)
                    .retryPolicy(new RetryNTimes(10, 1000))
                    .build()) {
                client.start();
                // Verify that the nodes for tracking the active queries were never created.
                assertThat(client.checkExists().forPath("/users/" + userA.toLowerCase() + "/" + tldQueryLogic + "/" + queryId)).isNull();
                assertThat(client.checkExists().forPath("/systems/" + system1 + "/" + tldQueryLogic + "/" + queryId)).isNull();
            }
            // @formatter:on
        }
    }

    /**
     * Verify {@link QueryLimiter#setup()} throws an exception if given a default user query limit less than 1.
     */
    @Test
    void testDefaultUserQueryLimitLessThanOne() {
        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());

        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultUserQueryLimit(0);
        config.setEnabled(true);
        limiter.setConfiguration(config);

        assertThatThrownBy(limiter::setup).isInstanceOf(IllegalArgumentException.class).hasMessage("Default user query limit must be greater than 0");
    }

    /**
     * Verify {@link QueryLimiter#setup()} throws an exception if given a default internal max cache size less than 1.
     */
    @Test
    void testDefaultQueryLimitLessThanOne() {
        QueryLimiter limiter = new QueryLimiter();
        limiter.setZookeeperConfig(server.getConnectString());

        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultUserQueryLimit(100);
        config.setDefaultSystemQueryLimit(5000);
        config.setInternalCacheMaxSize(0);
        config.setEnabled(true);
        limiter.setConfiguration(config);

        assertThatThrownBy(limiter::setup).isInstanceOf(IllegalArgumentException.class).hasMessage("Internal cache max size must be greater than 0");
    }

    /**
     * Verify that a user meeting the default max user query limit results in a met limit response.
     */
    @Test
    void testDefaultUserQueryLimitMet() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(5);
        config.setEnabled(true);
        givenConfig(config);

        startQueries(1, userA, system1, tldQueryLogic);
        startQueries(1, userA, system1, eventQueryLogic);
        startQueries(1, userA, system2, tldQueryLogic);
        startQueries(1, userA, system2, eventQueryLogic);

        // Verify that four active queries for the same user do not meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created five queries for the same user (across different servers and query logics), we have met a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 5 running queries");
    }

    /**
     * Verify that a system meeting the default max user query limit results in a met limit response.
     */
    @Test
    void testDefaultSystemQueryLimitMet() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(8);
        config.setDefaultUserQueryLimit(5);
        config.setEnabled(true);
        givenConfig(config);

        startQueries(2, userA, system1, tldQueryLogic);
        startQueries(2, userA, system1, eventQueryLogic);
        startQueries(3, userB, system1, tldQueryLogic);

        // Verify that seven active queries for the same system do not meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);

        startQueries(1, userB, system1, tldQueryLogic);

        // Verify that after we've created seven queries for the same system (across different users and query logics), we have met a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "System 'SYSTEM-01' has reached limit of 8 running queries");
    }

    /**
     * Verify that a user meeting a query logic group limit results in a met limit response.
     */
    @Test
    void testDefaultQueryLogicGroupLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(10);
        config.setEnabled(true);

        // Set a default limit of 3 TLDQueryLogic queries per user.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
        groupLimitConfig.setQueryLimit(3);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));
        givenConfig(config);

        startQueries(1, userA, system1, tldQueryLogic);
        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that two active TLDQueryLogics queries for userA do not meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created three queries for userA (across different servers), we have met a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 3 running queries for query logic group 'TLD'");

        // Assert that the user could still create queries for the EventQueryLogic.
        assertLimitNotMet(userA, system1, eventQueryLogic);
    }

    /**
     * Verify that a user meeting a custom max user query limit results in a met limit response.
     */
    @Test
    void testCustomUserQueryLimitMet() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(5);
        config.setEnabled(true);

        // Set a custom limit of 8 queries for userA.
        UserLimitConfiguration userConfig = new UserLimitConfiguration();
        userConfig.setUserDn(userA);
        userConfig.setQueryLimit(8);
        config.setUserConfigs(List.of(userConfig));
        givenConfig(config);

        startQueries(3, userA, system1, tldQueryLogic);
        startQueries(4, userA, system2, eventQueryLogic);

        // Verify that three active TLDQueryLogics queries for userA do not meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created four TLDQueryLogic queries for userA (across different servers), we have met a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 8 running queries");

        startQueries(5, userB, system1, eventQueryLogic);

        // Verify that userB, who has no custom limit, may not have more than 5 queries.
        assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 5 running queries");
    }

    /**
     * Verify that a user meeting a custom query logic group limit results in a met limit response.
     */
    @Test
    void testCustomUserQueryLogicGroupLimitMet() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(10);
        config.setEnabled(true);

        // Set a default limit of 5 TLDQueryLogic queries per user.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
        groupLimitConfig.setQueryLimit(5);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

        // Set a custom limit of 3 TLDQueryLogic queries for userA.
        UserLimitConfiguration userConfig = new UserLimitConfiguration();
        userConfig.setUserDn(userA);
        userConfig.setQueryLogicGroupLimits(Map.of("TLD", 3));
        config.setUserConfigs(List.of(userConfig));
        givenConfig(config);

        startQueries(1, userA, system1, tldQueryLogic);
        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that two active TLDQueryLogic queries for userA do not meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created three TLDQueryLogic queries for userA (across different servers), we have met a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 3 running queries for query logic group 'TLD'");

        // Verify that userB, who has no custom limit, may have up to 5 TLDQueryLogic queries.
        startQueries(4, userB, system1, tldQueryLogic);

        assertLimitNotMet(userB, system1, tldQueryLogic);

        startQueries(1, userB, system1, tldQueryLogic);

        assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 5 running queries for query logic group 'TLD'");
    }

    /**
     * Verify that a system meeting a custom query limit results in a met limit response.
     */
    @Test
    void testCustomServerQueryLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(25);
        config.setEnabled(true);

        // Set a limit of 10 queries for system2.
        SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
        systemConfig.setSystemPattern(system2);
        systemConfig.setQueryLimit(10);
        config.setSystemConfigs(List.of(systemConfig));
        givenConfig(config);

        startQueries(4, userA, system2, tldQueryLogic);
        startQueries(5, userB, system2, eventQueryLogic);

        // Verify that the nine active queries on system2 do not meet a limit.
        assertLimitNotMet(userA, system2, tldQueryLogic);

        startQueries(1, userA, system2, eventQueryLogic);

        // Verify that after we've created ten queries for system2 (across different users), we have met a limit.
        assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries");

        // Verify that system1 is not bound to the same limit as system2.
        startQueries(15, userB, system1, eventQueryLogic);
        assertLimitNotMet(userB, system1, tldQueryLogic);
    }

    /**
     * Verify that a system meeting a custom query logic group limit results in a met limit response.
     */
    @Test
    void testCustomServerQueryLogicGroupLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(50);
        config.setEnabled(true);

        // Set a default limit of 20 TLDQueryLogic queries per user.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
        groupLimitConfig.setQueryLimit(20);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

        // Set a limit of 10 queries for system2.
        SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
        systemConfig.setSystemPattern(system2);
        systemConfig.setQueryLogicGroupLimits(Map.of("TLD", 10));
        config.setSystemConfigs(List.of(systemConfig));
        givenConfig(config);

        startQueries(4, userA, system2, tldQueryLogic);
        startQueries(5, userB, system2, tldQueryLogic);

        // Verify that nine active TLDQueryLogic queries for system2 do not meet a limit.
        assertLimitNotMet(userA, system2, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created ten TLDQueryLogic queries on system2 (across different users), we have met a limit.
        assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries for query logic group 'TLD'");

        // Verify that system1, which has no custom limit, will not enforce any system-wide limits for TLDQueryLogic queries. TLDQueryLogic query limits outside
        // system2 only apply to individual users

        // Create enough queries for userA and userB to reach their individual limits for TLDQueryLogic queries.
        startQueries(15, userA, system1, tldQueryLogic);
        startQueries(15, userB, system1, tldQueryLogic);

        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
        assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
    }

    /**
     * Verify that even if a system does not count against the user query limit, the system meeting a custom query limit results in a met limit response.
     */
    @Test
    void testCustomServerQueryLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(25);
        config.setEnabled(true);

        // Set a limit of 10 queries for system2.
        SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
        systemConfig.setSystemPattern(system2);
        systemConfig.setQueryLimit(10);
        systemConfig.setCountsAgainstUserLimit(false);
        config.setSystemConfigs(List.of(systemConfig));
        givenConfig(config);

        startQueries(4, userA, system2, tldQueryLogic);
        startQueries(5, userB, system2, eventQueryLogic);

        // Verify that the nine active queries on system2 do not meet a limit.
        assertLimitNotMet(userA, system2, tldQueryLogic);

        startQueries(1, userA, system2, eventQueryLogic);

        // Verify that after we've created ten queries for system2 (across different users), we have met a limit.
        assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries");

        // Verify that system1 is not bound to the same limit as system2.
        startQueries(15, userB, system1, eventQueryLogic);
        assertLimitNotMet(userB, system1, tldQueryLogic);
    }

    /**
     * Verify that even if a system does not count against the user query limit, the system meeting a custom query logic group limit results in a met limit
     * response.
     */
    @Test
    void testCustomServerQueryLogicGroupLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(50);
        config.setEnabled(true);

        // Set a default limit of 20 TLDQueryLogic queries per user.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
        groupLimitConfig.setQueryLimit(20);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

        // Set a limit of 10 TLDQueryLogic queries for system2.
        SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
        systemConfig.setSystemPattern(system2);
        systemConfig.setCountsAgainstUserLimit(false);
        systemConfig.setQueryLogicGroupLimits(Map.of("TLD", 10));
        config.setSystemConfigs(List.of(systemConfig));
        givenConfig(config);

        startQueries(4, userA, system2, tldQueryLogic);
        startQueries(5, userB, system2, tldQueryLogic);

        // Verify that nine active TLDQueryLogic queries for system2 do not meet a limit.
        assertLimitNotMet(userA, system2, tldQueryLogic);

        startQueries(1, userA, system2, tldQueryLogic);

        // Verify that after we've created ten TLDQueryLogic queries on system2 (across different users), we have met a limit.
        assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries for query logic group 'TLD'");

        // Verify that system1, which has no custom limit, will not enforce any system-wide limits for TLDQueryLogic queries. TLDQueryLogic query limits outside
        // system2 only apply to individual users, and system2 queries do not count against the user limit.

        // Create 19 queries on system1. We should still be under the TLDQueryLogic limit since all previous queries do not count against the limit.
        startQueries(19, userA, system1, tldQueryLogic);
        startQueries(19, userB, system1, tldQueryLogic);

        assertLimitNotMet(userA, system1, tldQueryLogic);
        assertLimitNotMet(userB, system1, tldQueryLogic);

        // Create the 20th TLDQueryLogic query for each user on system1. This should meet the limit.
        startQueries(1, userA, system1, tldQueryLogic);
        startQueries(1, userB, system1, tldQueryLogic);

        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
        assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
    }

    /**
     * Verify that if a system does not count against the user query limit, that it does not count towards any limits other than:
     * <ul>
     * <li>The query limit specified for the system (custom if specified, or default otherwise).</li>
     * <li>Any custom query logic group limits specified for the limit.</li>
     * </ul>
     */
    @Test
    void testServerThatDoesNotCountAgainstUserLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(20);
        config.setEnabled(true);

        // Set a default limit of 20 TLDQueryLogic queries per user.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
        groupLimitConfig.setQueryLimit(10);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

        // Establish that system2 does not count against the user query limit.
        SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
        systemConfig.setSystemPattern(system2);
        systemConfig.setCountsAgainstUserLimit(false);
        config.setSystemConfigs(List.of(systemConfig));
        givenConfig(config);

        // Verify that after creating 25 TLDQueryLogic queries on system2, that we do not meet any limits since system2 does not count towards user limits.
        startQueries(25, userA, system2, tldQueryLogic);

        assertLimitNotMet(userA, system1, tldQueryLogic);
        assertLimitNotMet(userA, system2, tldQueryLogic);

        // Verify that after creating 10 TLDQueryLogic queries on system1 for userA, we meet the TLDQueryLogicLimit.
        startQueries(10, userA, system1, tldQueryLogic);

        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 10 running queries for query logic group 'TLD'");
    }

    /**
     * Verify that after meeting a limit, if we stop a query, we can create another query.
     */
    @Test
    void testCreatingQueryAfterStoppingQueryThatMetLimit() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(10);
        config.setEnabled(true);
        givenConfig(config);

        List<String> queryIds = startQueries(10, userA, system1, tldQueryLogic);

        // Verify that 10 queries for userA meets a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 10 running queries");

        // Stop one of the queries. Doesn't matter which, they're all for userA.
        getLimiter(system1).stopCountingQueryTowardsLimits(queryIds.get(0));

        // Verify that after stopping one of the queries, we no longer meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);
    }

    private QueryLimiter getLimiter(String system) {
        if (systemToLimiter.containsKey(system)) {
            return systemToLimiter.get(system);
        } else {
            QueryLimiter limiter = new QueryLimiter();
            limiter.setZookeeperConfig(server.getConnectString());
            limiter.setConfiguration(config);
            limiter.setHeartbeatCache(heartbeatCache);
            limiter.setup();
            systemToLimiter.put(system, limiter);
            return limiter;
        }
    }

    private List<String> startQueries(int numQueries, String userDn, String system, String queryLogic) throws Exception {
        List<String> queryIds = new ArrayList<>(numQueries);
        QueryLimiter limiter = getLimiter(system);
        for (int i = 0; i < numQueries; i++) {
            String queryId = UUID.randomUUID().toString();
            limiter.countQueryTowardsLimits(queryId, userDn, system, queryLogic);
            queryIds.add(queryId);
        }
        return queryIds;
    }

    private void assertLimitNotMet(String userDn, String system, String queryLogic) throws Exception {
        QueryLimiter limiter = getLimiter(system);
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.getMessage()).isNull();
        assertThat(response.metLimit()).isFalse();
    }

    private void assertLimitMet(String userDn, String system, String queryLogic, String message) throws Exception {
        QueryLimiter limiter = getLimiter(system);
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.getMessage()).isEqualTo(message);
        assertThat(response.metLimit()).isTrue();
    }

    private void givenConfig(QueryLimitConfiguration config) {
        this.config = config;
    }
}

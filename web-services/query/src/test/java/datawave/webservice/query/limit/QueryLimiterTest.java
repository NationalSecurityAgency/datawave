package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.json.JsonMapper;

import datawave.zookeeper.ZkClientBuilder;
import datawave.zookeeper.ZkPojoPublisher;
import datawave.zookeeper.ZkPojoPublisherImpl;

/**
 * Test cases for testing the functionality of {@link QueryLimiter}.
 */
class QueryLimiterTest {

    private static final String PUBLISHER_NAMESPACE = "pojoPublisher/queryLimitConfig";

    private static final String userA = "cn=testuserA, c=us";
    private static final String userB = "cn=testuserB, c=us";
    private static final String system1 = "SYSTEM-01";
    private static final String system2 = "SYSTEM-02";
    private static final String tldQueryLogic = "TLDQueryLogic";
    private static final String eventQueryLogic = "EventQueryLogic";

    private static final JsonMapper jsonMapper = new JsonMapper();

    private static String validJsonFile;
    private static String invalidYamlFile;

    private final Map<String,QueryLimiter> systemToLimiter = new HashMap<>();
    private QueryHeartbeatCache heartbeatCache;
    private QueryLimitConfiguration config;
    private QueryLimitConfigPublisher configPublisher;
    private TestingServer server;

    @BeforeAll
    static void beforeAll() throws Exception {
        ClassLoader classLoader = QueryLimiterTest.class.getClassLoader();
        validJsonFile = getAbsolutePath(classLoader, "queryLimits/valid_config.json");
        invalidYamlFile = getAbsolutePath(classLoader, "queryLimits/invalid_config.yaml");
    }

    /**
     * Return the absolute path for the given file as resolved by the classloader.
     *
     * @param classLoader
     *            the classloader
     * @param relativePath
     *            the relative path
     * @return the absolute path
     * @throws URISyntaxException
     *             if the URL cannot be converted to a URI
     */
    private static String getAbsolutePath(ClassLoader classLoader, String relativePath) throws URISyntaxException {
        URL url = classLoader.getResource(relativePath);
        if (url != null) {
            return Paths.get(url.toURI()).toAbsolutePath().toString();
        } else {
            throw new NullPointerException("Null URL returned for relative path '" + relativePath);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        heartbeatCache = new QueryHeartbeatCache();
        heartbeatCache.setup();
        configPublisher = new QueryLimitConfigPublisher();
        configPublisher.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()).withNamespace(PUBLISHER_NAMESPACE));
        configPublisher.setup();
    }

    @AfterEach
    void tearDown() throws IOException {
        heartbeatCache.shutdown();
        configPublisher.shutdown();
        systemToLimiter.clear();
        config = null;
        server.close();
    }

    /**
     * Verify {@link QueryLimiter#setup()} throws an exception if the query validation fails when a configuration is initially supplied via injection.
     */
    @Test
    void testConfigurationFailsValidation() {
        QueryLimiter limiter = new QueryLimiter();
        limiter.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
        limiter.setHeartbeatCache(heartbeatCache);

        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultUserQueryLimit(0);
        limiter.setConfiguration(config);

        assertThatThrownBy(limiter::setup).isInstanceOf(QueryLimitException.class).hasMessage("Activation failed.")
                        .hasRootCauseInstanceOf(IllegalArgumentException.class).hasRootCauseMessage("Default user query limit must be greater than 0");
    }

    /**
     * Verify that a user meeting the default max user query limit results in a met limit response.
     */
    @Test
    void testDefaultUserQueryLimitMet() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(5);
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
        givenConfig(config);

        List<String> queryIds = startQueries(10, userA, system1, tldQueryLogic);

        // Verify that 10 queries for userA meets a limit.
        assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 10 running queries");

        // Stop one of the queries. Doesn't matter which, they're all for userA.
        getLimiter(system1).stopCountingQueryTowardsLimits(queryIds.get(0));

        // Verify that after stopping one of the queries, we no longer meet a limit.
        assertLimitNotMet(userA, system1, tldQueryLogic);
    }

    /**
     * Verify that when a valid configuration is reloaded by the internal {@link ZkPojoPublisher}, the {@link QueryLimiter} is updated.
     */
    @Test
    void testValidConfigurationUpdate() throws Exception {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(5);
        givenConfig(config);

        QueryLimiter limiter = getLimiter(system1);

        // Create the path node. This should trigger a configuration reload that is passed to the limiter.
        try (CuratorFramework client = createReloaderClient()) {
            client.create().forPath("/path", validJsonFile.getBytes(StandardCharsets.UTF_8));
        }

        // Wait until we see a configuration from the limiter that does not match the original config.
        try {
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> limiter.getConfiguration().getDefaultSystemQueryLimit() != 100);
        } catch (Exception e) {
            fail("Timeout exceeded while waiting for limiter to be updated with new configuration.");
        }

        // The configuration loaded by the JSON file has a default user query limit of 100, and default system limit of 1000. Verify we see these changes
        // reflected in each limiter.
        QueryLimitConfiguration updatedConfig = limiter.getConfiguration();
        assertEquals(100, updatedConfig.getDefaultUserQueryLimit());
        assertEquals(1000, updatedConfig.getDefaultSystemQueryLimit());
    }

    /**
     * Verify that when an invalid configuration is supplied by the internal {@link ZkPojoPublisher}, the {@link QueryLimiter} is not updated, the original
     * configuration is preserved, and the error is written to the publisher attempt nodes.
     */
    @Test
    void testRevertingBackToOldConfiguration() throws Exception {
        QueryLimitConfiguration originalConfig = new QueryLimitConfiguration();
        originalConfig.setDefaultSystemQueryLimit(100);
        originalConfig.setDefaultUserQueryLimit(5);
        givenConfig(originalConfig);

        QueryLimiter limiter = getLimiter(system1);

        // Create the path node. This should trigger a configuration reload that is passed to the limiter.
        try (CuratorFramework client = createReloaderClient()) {
            client.create().forPath("/path", invalidYamlFile.getBytes(StandardCharsets.UTF_8));

            String serverIpAddress = InetAddress.getLocalHost().getHostAddress();
            String latestAttemptNode = "/attempts/" + serverIpAddress + "/latest";

            // Wait until we see that the attempt nodes were updated.
            try {
                Awaitility.await().atMost(4, TimeUnit.SECONDS).until(() -> client.checkExists().forPath(latestAttemptNode) != null);
            } catch (Exception e) {
                fail("Timeout exceeded while waiting for node " + latestAttemptNode + " to be created: " + e.getMessage());
            }

            // Verify that the attempt nodes were updated with the error.
            ZkPojoPublisherImpl.PublishAttempt publishAttempt = jsonMapper.readValue(getData(client, latestAttemptNode),
                            ZkPojoPublisherImpl.PublishAttempt.class);
            assertThat(publishAttempt.getStatus()).isEqualTo(ZkPojoPublisherImpl.Status.LISTENER_ERROR);
            assertThat(publishAttempt.getErrors().get(0).getMessage()).startsWith("Exception thrown by listener datawave.webservice.query.limit.QueryLimiter");
            assertThat(publishAttempt.getErrors().get(0).getStacktrace()).startsWith(
                            "datawave.webservice.query.limit.ConfigurationUpdateException: Failed to apply new configuration. Old configuration restored.");
        }

        // Verify that we reverted back to the old configuration.
        assertSame(originalConfig, limiter.getConfiguration());
        assertTrue(limiter.isEnforcingLimits());
    }

    private String getData(CuratorFramework client, String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            fail("Node " + path + " does not exist");
        }
        return new String(client.getData().forPath(path), StandardCharsets.UTF_8);
    }

    private QueryLimiter getLimiter(String system) {
        if (systemToLimiter.containsKey(system)) {
            return systemToLimiter.get(system);
        } else {
            QueryLimiter limiter = new QueryLimiter();
            limiter.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
            limiter.setConfiguration(config);
            limiter.setHeartbeatCache(heartbeatCache);
            limiter.setConfigPublisher(configPublisher);
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

    private CuratorFramework createReloaderClient() {
        // @formatter:off
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .namespace(PUBLISHER_NAMESPACE)
                .connectString(server.getConnectString())
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(60000)
                .retryPolicy(new RetryNTimes(10, 1000))
                .build();
        // @formatter:on
        client.start();
        return client;
    }
}

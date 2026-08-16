package datawave.webservice.query.limit;

import datawave.zookeeper.ZkClientBuilder;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class QueryLimiterImplIntegrationTest {
    
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
    
    @DisplayName("Method checkLimits() identifies when")
    @Nested
    class CheckLimitsBasicFunctionality {
        
        private static final String userA = "cn=testuserA, c=us";
        private static final String userB = "cn=testuserB, c=us";
        private static final String system1 = "SYSTEM-01";
        private static final String system2 = "SYSTEM-02";
        private static final String tldQueryLogic = "TLDQueryLogic";
        private static final String eventQueryLogic = "EventQueryLogic";
        
        private final Map<String,QueryLimiterImpl> systemToLimiter = new HashMap<>();
        private QueryLimitConfiguration limitConfig;
        
        @AfterEach
        void tearDown() {
            systemToLimiter.clear();
            limitConfig = null;
        }
        
        @DisplayName("default max user query limit is met")
        @Test
        void defaultUserQueryLimitMet() throws Exception {
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
        
        @DisplayName("default system query limit is met")
        @Test
        void defaultSystemQueryLimitMet() throws Exception {
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
        
        @DisplayName("default query logic group limit is met")
        @Test
        void defaultQueryLogicGroupLimit() throws Exception {
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
        
        @DisplayName("custom user query limit is met")
        @Test
        void customUserQueryLimitMet() throws Exception {
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
        
        @DisplayName("custom user query logic group limit is met")
        @Test
        void customUserQueryLogicGroupLimitMet() throws Exception {
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
        
        @DisplayName("custom server query limit is met")
        @Test
        void customServerQueryLimit() throws Exception {
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
        
        @DisplayName("custom server query logic group limit is met")
        @Test
        void customServerQueryLogicGroupLimit() throws Exception {
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
        @DisplayName("custom server (that does not count against user limits) query limit is met")
        @Test
        void customServerQueryLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
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
        @DisplayName("custom server (that does not count against user limits) query logic group limit is met")
        @Test
        void customServerQueryLogicGroupLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
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
        @DisplayName("server does not count towards user limits")
        @Test
        void serverThatDoesNotCountAgainstUserLimit() throws Exception {
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
        
        private QueryLimiterImpl getLimiter(String system) {
            if (systemToLimiter.containsKey(system)) {
                return systemToLimiter.get(system);
            } else {
                QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
                config.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
                config.setLimitConfiguration(limitConfig);
                
                QueryLimiterImpl limiter = new QueryLimiterImpl();
                limiter.setConfiguration(config);
                limiter.setup();
                systemToLimiter.put(system, limiter);
                return limiter;
            }
        }
        
        private List<String> startQueries(int numQueries, String userDn, String system, String queryLogic) throws Exception {
            List<String> queryIds = new ArrayList<>(numQueries);
            QueryLimiterImpl limiter = getLimiter(system);
            for (int i = 0; i < numQueries; i++) {
                String queryId = UUID.randomUUID().toString();
                limiter.markActive(queryId, userDn, system, queryLogic);
                queryIds.add(queryId);
            }
            return queryIds;
        }
        
        private void assertLimitNotMet(String userDn, String system, String queryLogic) throws Exception {
            QueryLimiterImpl limiter = getLimiter(system);
            QueryLimiterResponse response = limiter.checkLimits(userDn, system, queryLogic);
            assertThat(response.getMessage()).isNull();
            assertThat(response.metLimit()).isFalse();
        }
        
        private void assertLimitMet(String userDn, String system, String queryLogic, String message) throws Exception {
            QueryLimiterImpl limiter = getLimiter(system);
            QueryLimiterResponse response = limiter.checkLimits(userDn, system, queryLogic);
            assertThat(response.getMessage()).isEqualTo(message);
            assertThat(response.metLimit()).isTrue();
        }
        
        private void givenConfig(QueryLimitConfiguration config) {
            this.limitConfig = config;
        }
    }
}

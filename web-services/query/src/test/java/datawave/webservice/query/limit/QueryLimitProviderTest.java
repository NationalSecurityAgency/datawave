package datawave.webservice.query.limit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryLimitProviderTest {
    
    /**
     * Contains tests focused on validation of behavior when retrieving limits for systems.
     */
    private abstract static class BaseQueryLimitProviderTest {
        protected final List<UserConfiguration> userConfigs = new ArrayList<>();
        protected final List<SystemConfiguration> systemConfigs = new ArrayList<>();
        protected final List<QueryLogicGroupConfiguration> queryLogicGroupConfigs = new ArrayList<>();
        
        protected int defaultUserQueryLimit = 100;
        protected int defaultSystemQueryLimit = 5000;
        
        protected QueryLimitProvider provider;
        
        @AfterEach
        void tearDown() {
            userConfigs.clear();
            systemConfigs.clear();
            queryLogicGroupConfigs.clear();
            provider = null;
        }
        
        protected void initProvider() {
            QueryLimitProviderConfiguration config = new QueryLimitProviderConfiguration();
            config.setDefaultUserQueryLimit(defaultUserQueryLimit);
            config.setDefaultSystemQueryLimit(defaultSystemQueryLimit);
            config.setUserConfigs(userConfigs);
            config.setSystemConfigs(systemConfigs);
            config.setQueryLogicGroupConfigs(queryLogicGroupConfigs);
            provider = new QueryLimitProvider(config);
            provider.postConstruct();
        }
        
        protected void givenDefaultUserQueryLimit(int defaultSystemQueryLimit) {
            this.defaultUserQueryLimit = defaultSystemQueryLimit;
        }
        
        protected void givenDefaultSystemQueryLimit(int defaultSystemQueryLimit) {
            this.defaultSystemQueryLimit = defaultSystemQueryLimit;
        }
        
        protected void givenUserConfig(String userDn, Integer queryLimit, Map<String, Integer> queryLogicGroupLimits) {
            UserConfiguration config = new UserConfiguration();
            config.setUserDn(userDn);
            config.setQueryLimit(queryLimit);
            config.setQueryLogicGroupLimits(queryLogicGroupLimits);
            this.userConfigs.add(config);
        }
        
        protected void givenSystemConfig(String systemPattern, Integer queryLimit, Boolean countsAgainstUserLimit, Map<String, Integer> queryLogicGroupLimits) {
            SystemConfiguration config = new SystemConfiguration();
            config.setSystemPattern(systemPattern);
            config.setCountsAgainstsUserLimit(countsAgainstUserLimit);
            config.setQueryLimit(queryLimit);
            config.setQueryLogicGroupLimits(queryLogicGroupLimits);
            this.systemConfigs.add(config);
        }
        
        protected void givenQueryLogicGroupConfig(String groupName, String queryLogicPattern, int queryLimit) {
            QueryLogicGroupConfiguration config = new QueryLogicGroupConfiguration();
            config.setGroupName(groupName);
            config.setQueryLogicPattern(queryLogicPattern);
            config.setQueryLimit(queryLimit);
            queryLogicGroupConfigs.add(config);
        }
    }
    
    @Nested
    class DefaultLimitTests extends BaseQueryLimitProviderTest {
        
        /**
         * Verify that a null configuration results in an exception.
         */
        @Test
        void testNullConfig() {
            QueryLimitProvider provider = new QueryLimitProvider(null);
            assertThatThrownBy(provider::postConstruct).isInstanceOf(NullPointerException.class).hasMessageContaining("Configuration must not be null");
        }
        
        /**
         * Verify that when only the defaults are configured, that all limits returned adhere to them.
         */
        @Test
        void testDefaultsOnly() {
            initProvider();
            
            assertThat(provider.getDefaultUserQueryLimit()).isEqualTo(defaultUserQueryLimit);
            assertThat(provider.getDefaultSystemQueryLimit()).isEqualTo(defaultSystemQueryLimit);
            
            UserQueryLimit actualUserLimit = provider.getUserLimit("cn=testuser, ou=my department, o=my company, st=some-state, c=us");
            UserQueryLimit expectedUserLimit = UserQueryLimit.fromDefaults("cn=testuser, ou=my department, o=my company, st=some-state, c=us", 100);
            assertThat(actualUserLimit).isEqualTo(expectedUserLimit);
            
            SystemQueryLimit actualSystemLimit = provider.getSystemLimit("SYSTEM-01");
            SystemQueryLimit expectedSystemLimit = SystemQueryLimit.fromDefaults("SYSTEM-01", 5000);
            assertThat(actualSystemLimit).isEqualTo(expectedSystemLimit);
            
            Optional<QueryLogicGroupLimit> actualQueryLogicGroupLimit = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actualQueryLogicGroupLimit.isPresent()).isFalse();
        }
        
        /**
         * Verify that we do not allow a default user query limit that is 0 or less.
         */
        @Test
        void testInvalidDefaultUserQueryLimit() {
            givenDefaultUserQueryLimit(0);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Default user query limit must be greater than 0");
        }
        /**
         * Verify that we do not allow a default system query limit that is 0 or less.
         */
        @Test
        void testInvalidDefaultSystemQueryLimit() {
            givenDefaultSystemQueryLimit(0);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Default system query limit must be greater than 0");
        }
    
    }
    
    /**
     * Contains tests focused on validating behavior when retrieving limits for query logics.
     */
    @Nested
    class QueryLogicGroupLimitTests extends BaseQueryLimitProviderTest {
        
        /**
         * Verify that configurations with blank group names are forbidden.
         */
        @Test
        void testConfigWithBlankGroupName() {
            givenQueryLogicGroupConfig(" ", "TLDQueryLogic", 50);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Query logic group limit configuration given with blank group name");
        }
        
        /**
         * Verify that multiple configurations with the same group name are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameGroupName() {
            givenQueryLogicGroupConfig("TLD", "TLDQueryLogic", 50);
            givenQueryLogicGroupConfig("TLD", "TLD*", 25);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query logic group configurations given with group name 'TLD'");
        }
        /**
         * Verify that configurations with negative limits are forbidden.
         */
        @Test
        void testConfigWithNegativeLimit() {
            givenQueryLogicGroupConfig("TLD", "TLDQueryLogic", -1);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Negative limit given for query logic group 'TLD'");
        }
        
        /**
         * Verify that configurations with blank query logic patterns are forbidden.
         */
        @Test
        void testConfigWithBlankQueryLogicPattern() {
            givenQueryLogicGroupConfig("TLD", " ", 50);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Blank query logic pattern given for query logic group 'TLD'");
        }
        
        /**
         * Verify that query logic patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableQueryLogicPattern() {
            givenQueryLogicGroupConfig("TLD", "TLD[", 50);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid regex in query logic pattern 'TLD[' for query logic group 'TLD'");
        }
        
        /**
         * Verify that we support {@code *} as a wildcard pattern.
         */
        @Test
        void testImpliedWildcardQueryLogicPattern() {
            givenQueryLogicGroupConfig("ImpliedWildcard", "*", 25);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("ImpliedWildcard", 25));
        }
        
        /**
         * Verify that an explicit wildcard pattern returns a limit for any query logic.
         */
        @Test
        void testExplicitWildcardQueryLogicPattern() {
            givenQueryLogicGroupConfig("ExplicitWildcard", ".*", 25);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("ExplicitWildcard", 25));
        }
        
        /**
         * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
         */
        @Test
        void testPartialMatchOverridesWildcardMatch() {
            givenQueryLogicGroupConfig("Wildcard", ".*", 25);
            givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("PartialMatch", 40));
        }
        
        /**
         * Verify that when we have multiple partial matches, that the one with the lowest limit is returned.
         */
        @Test
        void testMultiplePartialMatchesReturnsLowestLimit() {
            givenQueryLogicGroupConfig("PartialMatch1", "TLD.*", 25);
            givenQueryLogicGroupConfig("PartialMatch2", ".*TLD.*", 40);
            givenQueryLogicGroupConfig("PartialMatch3", ".*LD.*", 15);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("PartialMatch3", 15));
        }
        
        /**
         * Verify that when we have an exact match, partial match and a wildcard match, that the exact match is returned.
         */
        @Test
        void testExactMatchOverridesPartialAndWildcardMatch() {
            givenQueryLogicGroupConfig("Wildcard", ".*", 25);
            givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);
            givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 55);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("ExactMatch", 55));
        }
        
        /**
         * Verify that when we have multiple exact matches, that the one with the lowest limit is returned.
         */
        @Test
        void testMultipleExactMatchesReturnsLowestLimit() {
            givenQueryLogicGroupConfig("ExactMatch1", "TLDQueryLogic", 25);
            givenQueryLogicGroupConfig("ExactMatch2", "TLDQueryLogic", 40);
            givenQueryLogicGroupConfig("ExactMatch3", "TLDQueryLogic", 55);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> actual = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(actual.isPresent()).isTrue();
            assertThat(actual.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("ExactMatch1", 25));
        }
        
        /**
         * Verify that when there is no match against a query logic group, that an empty optional is returned.
         */
        @Test
        void testNoMatch() {
            givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);
            givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 55);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> optional = provider.getQueryLogicGroupLimit("TLDQueryLogic");
            assertThat(optional.isPresent()).isTrue();
        }
        
        /**
         * Verify that when attempting to fetch the best overridden limit, that if there is no match, then an empty optional is returned.
         */
        @Test
        void testOverriddenLimitWithNoMatch() {
            givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);
            givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 55);
         
            initProvider();
            
            Optional<QueryLogicGroupLimit> optional = provider.getOverriddenQueryLogicGroupLimit("SYSTEM_01", "OtherQueryLogic", Map.of("ExactMatch", 100));
            assertThat(optional.isPresent()).isFalse();
        }
        
        /**
         * Verify that when there are multiple matches for overridden limits, that the best match (that was present in the map of overridden limits) with the
         * lowest limit is returned.
         */
        @Test
        void testOverriddenPartialMatchesWithNonOverriddenExactMatch() {
            givenQueryLogicGroupConfig("PartialMatch01", "TLD.*", 200);
            givenQueryLogicGroupConfig("PartialMatch02", "TL.*", 75);
            givenQueryLogicGroupConfig("PartialMatch03", "TLDQuery[Ll]ogic", 300);
            givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 5);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> optional = provider.getOverriddenQueryLogicGroupLimit("SYSTEM_01", "TLDQueryLogic", Map.of("PartialMatch01", 50, "PartialMatch03", 20));
            assertThat(optional.isPresent()).isTrue();
            
            // We should not receive the group ExactMatch, since we are filtering our results by the groups present in the map of overridden limits.
            assertThat(optional.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("PartialMatch03", 20));
        }
        
        /**
         * Verify that when there are multiple matches for overridden limits, that the best match (that was present in the map of overridden limits) with the
         * lowest limit is returned.
         */
        @Test
        void testOverriddenPartialMatchesWithOverriddenExactMatch() {
            givenQueryLogicGroupConfig("PartialMatch01", "TLD.*", 200);
            givenQueryLogicGroupConfig("PartialMatch02", "TL.*", 75);
            givenQueryLogicGroupConfig("PartialMatch03", "TLDQuery[Ll]ogic", 300);
            givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 5);
            
            initProvider();
            
            Optional<QueryLogicGroupLimit> optional = provider.getOverriddenQueryLogicGroupLimit("SYSTEM_01", "TLDQueryLogic", Map.of("PartialMatch01", 50, "PartialMatch03", 20, "ExactMatch", 200));
            assertThat(optional.isPresent()).isTrue();
            
            // We should receive ExactMatch this time, since it was present in the map of overridden limits.
            assertThat(optional.get()).isEqualTo(QueryLogicGroupLimit.fromConfig("ExactMatch", 200));
        }
    }
    
    /**
     * Contains tests focused on validation of behavior when retrieving limits for users.
     */
    @Nested
    class UserLimitTests extends BaseQueryLimitProviderTest {
        
        /**
         * Verify that blank user DNs are forbidden.
         */
        @Test
        void testConfigWithBlankDn() {
            givenUserConfig("  ", null, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("User query limit configuration given with blank user DN");
        }
        
        /**
         * Verify that multiple configurations with the same user DN are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameUserDn() {
            givenUserConfig("cn=test user, c=us", 100, null);
            givenUserConfig("cn=test user, c=us", 200, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Multiple query limit configurations specified for user 'cn=test user, c=us'");
        }
        
        /**
         * Verify that negative query limits are forbidden.
         */
        @Test
        void testConfigWithNegativeLimit() {
            givenUserConfig("cn=test user, c=us", -1, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Negative user query limit given for user 'cn=test user, c=us'");
        }
        
        /**
         * Verify that user configurations with query logic group names that do not match any existing query logic group configuration are forbidden.
         */
        @Test
        void testUserConfigurationWithNonExistentQueryLogicGroupName() {
            givenUserConfig("cn=test user, c=us", null, Map.of("TLD", 200));
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Non-existent query logic groups specified for query limit configuration for user 'cn=test user, c=us': [TLD]");
        }
        
        /**
         * Verify that user configurations that override various aspects of query limits are extracted correctly.
         */
        @Test
        void testUserConfigurations() {
            // Provide some query logic groups.
            givenQueryLogicGroupConfig("TLDLogics", "TLDQueryLogic", 50);
            givenQueryLogicGroupConfig("ExpensiveLogics", "Expensive*", 10);
            
            // A user configuration that overrides the user query limit, but does not override any query logic group limits.
            givenUserConfig("cn=userA, c=us", 200, null);
            
            // A user configuration that does not override the user query limit, but does override the TLDLogics group limit.
            givenUserConfig("cn=userB, c=us", null, Map.of("TLDLogics", 200));
            
            // A user configuration that overrides both the user query limit, and the TLDLogics group limit.
            givenUserConfig("cn=userC, c=us", 50, Map.of("TLDLogics", 10));
            
            initProvider();
            
            // Assert that a user with no configuration has the default limits.
            assertThat(provider.getUserLimit("cn=userD, c=us")).isEqualTo(UserQueryLimit.fromDefaults("cn=userD, c=us", defaultUserQueryLimit));
            
            // Assert userA.
            assertThat(provider.getUserLimit("cn=userA, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userA, c=us", 200, Map.of()));
            
            // Assert userB.
            assertThat(provider.getUserLimit("cn=userB, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userB, c=us", defaultUserQueryLimit, Map.of("TLDLogics", 200)));
            
            // Assert userC.
            assertThat(provider.getUserLimit("cn=userC, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userC, c=us", 50, Map.of("TLDLogics", 10)));
        }
    
    }
    
    @Nested
    class SystemLimitTests extends BaseQueryLimitProviderTest {
        
        /**
         * Verify that configurations with blank system patterns are forbidden.
         */
        @Test
        void testConfigWithBlankSystemPattern() {
            givenSystemConfig(" ", 10, true, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System query limit configuration specified with blank system pattern");
        }
        
        /**
         * Verify that configurations with regex system patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableSystemPattern() {
            givenSystemConfig("SYS[", 10, true, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Invalid regex in system pattern 'SYS['");
        }
        
        /**
         * Verify that multiple configurations using the same system patterns are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameSystemPattern() {
            givenSystemConfig("SYSTEM_01*", 10, true, null);
            givenSystemConfig("SYSTEM_01*", 10, true, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query limit configurations specified with system pattern 'SYSTEM_01*'");
        }
        
        /**
         * Verify that configurations with conflicting system patterns that are equivalent exact matches are forbidden.
         */
        @Test
        void testEquivalentExactMatchPatterns() {
            givenSystemConfig("SYSTEM_01", 10, true, null); // Literals only.
            givenSystemConfig("SYSTEM\\_01", 10, true, null); // Literals and escaped literals.
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern 'SYSTEM\\_01' will resolve to an exact match that is equivalent to system pattern 'SYSTEM_01' from "
                                            + "another system configuration.");
        }
        
        /**
         * Verify that configurations with negative query limits are forbidden.
         */
        @Test
        void testNegativeQueryLimit() {
            givenSystemConfig("SYSTEM_01*", -1, true, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Negative query limit specified for system pattern 'SYSTEM_01*'");
        }
        
        /**
         * Verify that configurations with an implied wildcard system pattern {@code *} that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testImpliedWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig("*", 10, false, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern '*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }
        
        /**
         * Verify that configurations with an explicit wildcard system pattern that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testExplicitWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig(".*", 10, false, null);
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern '.*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }
        
        /**
         * Verify that configurations with non-existent query logic groups are forbidden.
         */
        @Test
        void testConfigurationWithNonExistentQueryLogicGroup() {
            givenSystemConfig("SYSTEM-01", 100, true, Map.of("TLD", 200));
            
            assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Non-existent query logic groups given for system pattern 'SYSTEM-01': [TLD]");
        }
        
        /**
         * Verify that we support {@code *} as a wildcard pattern.
         */
        @Test
        void testImpliedWildcardQueryLogicPattern() {
            givenSystemConfig("*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-01")).isEqualTo(SystemQueryLimit.fromConfig("*", 100, true, null));
        }
        
        /**
         * Verify that an explicit wildcard pattern returns a limit for any system.
         */
        @Test
        void testExplicitWildcardQueryLogicPattern() {
            givenSystemConfig(".*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-01")).isEqualTo(SystemQueryLimit.fromConfig(".*", 100, true, null));
        }
        
        /**
         * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
         */
        @Test
        void testPartialMatchOverridesWildcardMatch() {
            givenSystemConfig("SYSTEM-.*", 75, false, null);
            givenSystemConfig(".*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-.*")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-.*", 75, false, null));
        }
        
        /**
         * Verify that when we have multiple partial matches, that the one with the lowest limit is returned.
         */
        @Test
        void testMultiplePartialMatchesReturnsLowestLimit() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-AB.*", 40, true, null);
            givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
            givenSystemConfig(".*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-ABC.*", 30, true, null));
        }
        
        /**
         * Verify that when we have an exact match, partial match and a wildcard match, that the exact match is returned.
         */
        @Test
        void testExactMatchOverridesPartialAndWildcardMatch() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-AB.*", 40, true, null);
            givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
            givenSystemConfig("SYSTEM-ABC", 400, true, null);
            givenSystemConfig(".*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-ABC", 400, true, null));
        }
        
        /**
         * Verify that a system pattern that consists only of literals and escaped literals is treated as an exact match.
         */
        @Test
        void testExactMatchThatHasEscapedLiterals() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-AB.*", 40, true, null);
            givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
            givenSystemConfig("SYSTEM\\-ABC", 400, true, null);
            givenSystemConfig(".*", 100, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM\\-ABC", 400, true, null));
        }
        
        /**
         * Verify that when there is no match, that default limits are returned.
         */
        @Test
        void testNoMatch() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-AB.*", 40, true, null);
            givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
            givenSystemConfig("SYSTEM-ABC", 400, true, null);
            
            initProvider();
            
            assertThat(provider.getSystemLimit("SYSTEM-ECHO")).isEqualTo(SystemQueryLimit.fromDefaults("SYSTEM-ECHO", defaultSystemQueryLimit));
        }
        
        /**
         * Verify that when there is no configured match for a system, that queries count against the user limit for the system.
         */
        @Test
        void testCountsAgainstUserLimitNoMatch() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-ABC", 400, true, null);
            
            initProvider();
            
            assertThat(provider.systemCountsAgainstUserLimit("SYSTEM-ECHO")).isTrue();
        }
        
        /**
         * Verify that an exact match where queries count against the user limits returns true.
         */
        @Test
        void testCountsAgainstUserLimitWithExactMatchWithTrue() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-ABC", 400, true, null);
            
            initProvider();
            
            assertThat(provider.systemCountsAgainstUserLimit("SYSTEM-ABC")).isTrue();
        }
        
        /**
         * Verify that an exact match where queries count against the user limits returns false.
         */
        @Test
        void testCountsAgainstUserLimitWithExactMatchWithFalse() {
            givenSystemConfig("SYSTEM-A.*", 75, true, null);
            givenSystemConfig("SYSTEM-ABC", 400, false, null);
            
            initProvider();
            
            assertThat(provider.systemCountsAgainstUserLimit("SYSTEM-ABC")).isFalse();
        }
        
        /**
         * Verify that when there is no exact match and there are multiple partial matches where at least one counts queries against the user limit, true is
         * returned.
         */
        @Test
        void testCountsAgainstUserLimitWithPartialMatchWithTrue() {
            givenSystemConfig("SYSTEM-A.*", 75, true, null);
            givenSystemConfig("SYSTEM-AB.*", 400, false, null);
            givenSystemConfig("SYSTEM-ABC*", 400, false, null);
            
            initProvider();
            
            assertThat(provider.systemCountsAgainstUserLimit("SYSTEM-ABC")).isTrue();
        }
        
        /**
         * Verify that when there is no exact match and there are multiple partial matches where all do not count queries against the user limit, false is
         * returned.
         */
        @Test
        void testCountsAgainstUserLimitWithAllPartialMatchWithFalse() {
            givenSystemConfig("SYSTEM-A.*", 75, false, null);
            givenSystemConfig("SYSTEM-AB.*", 400, false, null);
            givenSystemConfig("SYSTEM-ABC.*", 400, false, null);
            
            initProvider();
            
            assertThat(provider.systemCountsAgainstUserLimit("SYSTEM-ABC")).isFalse();
        }
    }
}

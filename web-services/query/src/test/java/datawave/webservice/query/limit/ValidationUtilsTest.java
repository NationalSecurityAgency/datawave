package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ValidationUtils}.
 */
class ValidationUtilsTest {

    /**
     * Tests for {@link ValidationUtils#validateQueryLimitConfig(QueryLimitConfiguration)}.
     */
    @Nested
    class QueryLimitConfigurationValidationTests {

        /**
         * Verify the default user query limit cannot be less than 1.
         */
        @Test
        void testDefaultUserQueryLimitLessThanOne() {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultUserQueryLimit(0);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLimitConfig(config)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Default user query limit must be greater than 0");
        }

        /**
         * Verify the default internal max cache size cannot be less than 1.
         */
        @Test
        void testDefaultQueryLimitLessThanOne() {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultUserQueryLimit(100);
            config.setDefaultSystemQueryLimit(5000);
            config.setInternalCacheMaxSize(0);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLimitConfig(config)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Internal cache max size must be greater than 0");
        }
    }

    /**
     * Tests for {@link ValidationUtils#validateQueryLogicGroupConfigs(Collection)}.
     */
    @Nested
    class QueryLogicGroupLimitConfigurationValidationTests {
        private final List<QueryLogicGroupLimitConfiguration> configs = new ArrayList<>();

        @BeforeEach
        void setUp() {
            configs.clear();
        }

        /**
         * Verify that configurations with blank group names are forbidden.
         */
        @Test
        void testConfigWithBlankGroupName() {
            givenConfig(" ", "TLDQueryLogic", 50);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLogicGroupConfigs(configs))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Query logic group limit configuration given with blank group name");
        }

        /**
         * Verify that multiple configurations with the same group name are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameGroupName() {
            givenConfig("TLD", "TLDQueryLogic", 50);
            givenConfig("TLD", "TLD*", 25);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLogicGroupConfigs(configs))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Multiple query logic group configurations given with group name 'TLD'");
        }

        /**
         * Verify that configurations with negative limits are forbidden.
         */
        @Test
        void testConfigWithNegativeLimit() {
            givenConfig("TLD", "TLDQueryLogic", -1);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLogicGroupConfigs(configs))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Negative limit given for query logic group 'TLD'");
        }

        /**
         * Verify that configurations with blank query logic patterns are forbidden.
         */
        @Test
        void testConfigWithBlankQueryLogicPattern() {
            givenConfig("TLD", " ", 50);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLogicGroupConfigs(configs))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Blank query logic pattern given for query logic group 'TLD'");
        }

        /**
         * Verify that query logic patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableQueryLogicPattern() {
            givenConfig("TLD", "TLD[", 50);

            assertThatThrownBy(() -> ValidationUtils.validateQueryLogicGroupConfigs(configs))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid regex in query logic pattern 'TLD[' for query logic group 'TLD'");
        }

        private void givenConfig(String groupName, String queryLogicPattern, int queryLimit) {
            QueryLogicGroupLimitConfiguration config = new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit);
            configs.add(config);
        }
    }

    /**
     * Tests for {@link ValidationUtils#validateUserLimitConfigs(Collection)}.
     */
    @Nested
    class UserLimitConfigurationValidationTests {
        private final List<UserLimitConfiguration> configs = new ArrayList<>();

        @BeforeEach
        void setUp() {
            configs.clear();
        }

        /**
         * Verify that blank user DNs are forbidden.
         */
        @Test
        void testConfigWithBlankDn() {
            givenUserConfig("  ", null);

            assertThatThrownBy(() -> ValidationUtils.validateUserLimitConfigs(configs)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("User query limit configuration given with blank user DN");
        }

        /**
         * Verify that multiple configurations with the same user DN are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameUserDn() {
            givenUserConfig("cn=test user, c=us", 100);
            givenUserConfig("cn=test user, c=us", 200);

            assertThatThrownBy(() -> ValidationUtils.validateUserLimitConfigs(configs)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query limit configurations specified for user 'cn=test user, c=us'");
        }

        /**
         * Verify that negative query limits are forbidden.
         */
        @Test
        void testConfigWithNegativeLimit() {
            givenUserConfig("cn=test user, c=us", -1);

            assertThatThrownBy(() -> ValidationUtils.validateUserLimitConfigs(configs)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Negative user query limit given for user 'cn=test user, c=us'");
        }

        private void givenUserConfig(String userDn, Integer queryLimit) {
            this.configs.add(new UserLimitConfiguration(userDn, queryLimit, null));
        }
    }

    /**
     * Tests for {@link ValidationUtils#validateSystemLimitConfigs(Collection, long)}.
     */
    @Nested
    class SystemLimitConfigurationValidationTests {

        private final List<SystemLimitConfiguration> configs = new ArrayList<>();

        @BeforeEach
        void setUp() {
            configs.clear();
        }

        /**
         * Verify that configurations with blank system patterns are forbidden.
         */
        @Test
        void testConfigWithBlankSystemPattern() {
            givenSystemConfig(" ", 10, true, null);

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("System query limit configuration specified with blank system pattern");
        }

        /**
         * Verify that configurations with regex system patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableSystemPattern() {
            givenSystemConfig("SYS[", 10, true, null);

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid regex in system pattern 'SYS['");
        }

        /**
         * Verify that multiple configurations using the same system patterns are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameSystemPattern() {
            givenSystemConfig("SYSTEM_01*", 10, true, null);
            givenSystemConfig("SYSTEM_01*", 10, true, null);

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query limit configurations specified with system pattern 'SYSTEM_01*'");
        }

        /**
         * Verify that configurations with conflicting system patterns that are equivalent exact matches are forbidden.
         */
        @Test
        void testEquivalentExactMatchPatterns() {
            givenSystemConfig("SYSTEM_01", 10, true, null); // Literals only.
            givenSystemConfig("SYSTEM\\_01", 10, true, null); // Literals and escaped literals.

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern 'SYSTEM\\_01' will resolve to an exact match that is equivalent to system pattern 'SYSTEM_01' from "
                                            + "another system configuration.");
        }

        /**
         * Verify that configurations with an implied wildcard system pattern {@code *} that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testImpliedWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig("*", 10, false, null);

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern '*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }

        /**
         * Verify that configurations with an explicit wildcard system pattern that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testExplicitWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig(".*", 10, false, null);

            assertThatThrownBy(() -> ValidationUtils.validateSystemLimitConfigs(configs, 200))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern '.*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }

        private void givenSystemConfig(String systemPattern, Integer queryLimit, Boolean countsAgainstUserLimit, Map<String,Integer> queryLogicGroupLimits) {
            SystemLimitConfiguration config = new SystemLimitConfiguration(systemPattern, countsAgainstUserLimit, queryLimit, queryLogicGroupLimits);
            configs.add(config);
        }

    }
}

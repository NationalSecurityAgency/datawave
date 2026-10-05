package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SystemLimitProviderTest {

    private static final int defaultLimit = 5000;
    private static final int maxCacheSize = 200;
    private SystemLimitProvider provider;
    private final List<SystemLimitConfiguration> systemConfigs = new ArrayList<>();
    private final List<QueryLogicGroupLimitConfiguration> groupConfigs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        provider = null;
        systemConfigs.clear();
        groupConfigs.clear();
    }

    /**
     * Tests for the initialization of a {@link SystemLimitProvider}.
     */
    @Nested
    class InitializationTests {

        /**
         * Verify that configurations with blank system patterns are forbidden.
         */
        @Test
        void testConfigWithBlankSystemPattern() {
            givenSystemConfig(" ", 10, true, null);

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System query limit configuration specified with blank system pattern");
        }

        /**
         * Verify that configurations with regex system patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableSystemPattern() {
            givenSystemConfig("SYS[", 10, true, null);

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Invalid regex in system pattern 'SYS['");
        }

        /**
         * Verify that multiple configurations using the same system patterns are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameSystemPattern() {
            givenSystemConfig("SYSTEM_01*", 10, true, null);
            givenSystemConfig("SYSTEM_01*", 10, true, null);

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query limit configurations specified with system pattern 'SYSTEM_01*'");
        }

        /**
         * Verify that configurations with conflicting system patterns that are equivalent exact matches are forbidden.
         */
        @Test
        void testEquivalentExactMatchPatterns() {
            givenSystemConfig("SYSTEM_01", 10, true, null); // Literals only.
            givenSystemConfig("SYSTEM\\_01", 10, true, null); // Literals and escaped literals.

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("System pattern 'SYSTEM\\_01' will resolve to an exact match that is equivalent to system pattern 'SYSTEM_01' from "
                                            + "another system configuration.");
        }

        /**
         * Verify that configurations with an implied wildcard system pattern {@code *} that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testImpliedWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig("*", 10, false, null);

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage(
                            "System pattern '*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }

        /**
         * Verify that configurations with an explicit wildcard system pattern that are configured to not apply to user limits are forbidden.
         */
        @Test
        void testExplicitWildcardSystemPatternThatDoesNotApplyToUserLimit() {
            givenSystemConfig(".*", 10, false, null);

            assertThatThrownBy(SystemLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage(
                            "System pattern '.*' is wildcard-only and may not be used to override whether queries count against user limits to false");
        }

        /**
         * Verify that we are able to get the correct default system query limit.
         */
        @Test
        void testGetDefaultSystemQueryLimit() {
            initProvider();

            assertThat(provider.getDefaultSystemQueryLimit()).isEqualTo(defaultLimit);
        }

        /**
         * Verify that a default system query limit less than -1 is set to the value of {@link QueryLimitConstants#NO_LIMIT}.
         */
        @Test
        void testDefaultSystemLessThanNegativeOne() {
            SystemLimitProvider provider = new SystemLimitProvider(-3, maxCacheSize, Set.of(), new QueryLogicGroupLimitProvider(maxCacheSize, Set.of()));
            assertThat(provider.getDefaultSystemQueryLimit()).isEqualTo(QueryLimitConstants.NO_LIMIT);
        }
    }

    /**
     * Tests for {@link SystemLimitProvider#getCustomLimits(String)}.
     */
    @Nested
    class GetCustomLimitTests {

        /**
         * Verify that when a system does not have any custom limits, a custom limit cannot be fetched, and it counts against the user query limit.
         */
        @Test
        void testSystemWithoutCustomLimits() {
            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM_01");
            assertThat(optional).isEmpty();
            assertThat(provider.countsAgainstUserLimit("SYSTEM_01")).isTrue();
        }

        /**
         * Verify that a system with custom limits is parsed correctly.
         */
        @Test
        void testSystemWithCustomLimits() {
            givenGroupConfig("TLD", "TLDQueryLogic", 50);
            givenSystemConfig("SYSTEM_01", 100, false, Map.of("TL.*", 20));
            givenSystemConfig("SYSTEM_02", -3, true, Map.of());

            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM_01");
            assertThat(optional).isNotEmpty();

            // Assert configuration for SYSTEM_01.
            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM_01");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(100);
            assertThat(systemLimits.countsAgainstUserLimit()).isFalse();
            assertThat(systemLimits.getBestGroupLimits("TLDQueryLogic")).containsExactly(Map.entry("TLD", 20));

            // Assert configuration for SYSTEM_02.
            optional = provider.getCustomLimits("SYSTEM_02");
            assertThat(optional).isNotEmpty();
            systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM_02");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(QueryLimitConstants.NO_LIMIT);
            assertThat(systemLimits.countsAgainstUserLimit()).isTrue();
            assertThat(systemLimits.getBestGroupLimits("TLDQueryLogic")).isEmpty();
        }

        /**
         * Verify that we support {@code *} as a wildcard pattern.
         */
        @Test
        void testImpliedWildcardQueryLogicPattern() {
            givenSystemConfig("*", 100, true, null);

            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM_01");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("*");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(100);
            assertThat(systemLimits.countsAgainstUserLimit()).isTrue();
            assertThat(systemLimits.getGroupLimitOverrides().isEmpty()).isTrue();
        }

        /**
         * Verify that an explicit wildcard pattern returns a limit for any system.
         */
        @Test
        void testExplicitWildcardQueryLogicPattern() {
            givenSystemConfig(".*", 100, true, null);

            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM_01");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo(".*");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(100);
            assertThat(systemLimits.countsAgainstUserLimit()).isTrue();
            assertThat(systemLimits.getGroupLimitOverrides().isEmpty()).isTrue();
        }

        /**
         * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
         */
        @Test
        void testPartialMatchOverridesWildcardMatch() {
            givenSystemConfig("SYSTEM-.*", 75, false, null);
            givenSystemConfig(".*", 100, true, null);

            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM-01");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-.*");
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

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM-ABC");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-ABC.*");
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

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM-ABC");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-ABC");
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

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM-ABC");
            assertThat(optional).isNotEmpty();

            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM\\-ABC");
        }

        /**
         * Verify that system limits are not silently dropped when two or more system configurations have matchers of the same type, equal limits, equal
         * countsAgainstUserLimit, and equal group overrides.
         */
        @Test
        void testSystemsWithSameMatcherTypeAndEqualLimitsAreNotDropped() {
            givenSystemConfig("SYSTEM-A", 15, true, null);
            givenSystemConfig("SYSTEM-B", 15, true, null);
            givenSystemConfig("SYSTEM-C.*", 30, false, null);
            givenSystemConfig("SYSTEM-D.*", 30, false, null);
            givenSystemConfig(".*", 100, true, null);

            initProvider();

            Optional<SystemLimits> optional = provider.getCustomLimits("SYSTEM-A");
            assertThat(optional).isNotEmpty();
            SystemLimits systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-A");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(15);

            optional = provider.getCustomLimits("SYSTEM-B");
            assertThat(optional).isNotEmpty();
            systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-B");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(15);

            optional = provider.getCustomLimits("SYSTEM-C");
            assertThat(optional).isNotEmpty();
            systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-C.*");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(30);

            optional = provider.getCustomLimits("SYSTEM-D");
            assertThat(optional).isNotEmpty();
            systemLimits = optional.get();
            assertThat(systemLimits.getSystemPattern()).isEqualTo("SYSTEM-D.*");
            assertThat(systemLimits.getQueryLimit()).isEqualTo(30);

        }
    }

    /**
     * Verify that configurations with an implied wildcard system pattern {@code *} that are configured to not apply to user limits are forbidden.
     */
    @Test
    void testImpliedWildcardLimit() {
        givenSystemConfig("*", 500, true, null);
    }

    private void givenGroupConfig(String groupName, String queryLogicPattern, int queryLimit) {
        this.groupConfigs.add(new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit));
    }

    private void givenSystemConfig(String systemPattern, Integer queryLimit, Boolean countsAgainstUserLimit, Map<String,Integer> queryLogicGroupLimits) {
        SystemLimitConfiguration config = new SystemLimitConfiguration(systemPattern, countsAgainstUserLimit, queryLimit, queryLogicGroupLimits);
        this.systemConfigs.add(config);
    }

    private void initProvider() {
        QueryLogicGroupLimitProvider groupProvider = new QueryLogicGroupLimitProvider(maxCacheSize, groupConfigs);
        this.provider = new SystemLimitProvider(defaultLimit, maxCacheSize, systemConfigs, groupProvider);
    }
}

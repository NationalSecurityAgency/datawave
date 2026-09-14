package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueryLogicGroupLimitProviderTest {

    private QueryLogicGroupLimitProvider provider;
    private final List<QueryLogicGroupLimitConfiguration> configs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        provider = null;
        configs.clear();
    }

    /**
     * Tests for the initialization of a {@link QueryLogicGroupLimitProvider}.
     */
    @Nested
    class InitializationTest {

        /**
         * Verify that configurations with blank group names are forbidden.
         */
        @Test
        void testConfigWithBlankGroupName() {
            givenConfig(" ", "TLDQueryLogic", 50);

            assertThatThrownBy(QueryLogicGroupLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Query logic group limit configuration given with blank group name");
        }

        /**
         * Verify that multiple configurations with the same group name are forbidden.
         */
        @Test
        void testMultipleConfigsWithSameGroupName() {
            givenConfig("TLD", "TLDQueryLogic", 50);
            givenConfig("TLD", "TLD*", 25);

            assertThatThrownBy(QueryLogicGroupLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Multiple query logic group configurations given with group name 'TLD'");
        }

        /**
         * Verify that configurations with negative limits are forbidden.
         */
        @Test
        void testConfigWithNegativeLimit() {
            givenConfig("TLD", "TLDQueryLogic", -1);

            assertThatThrownBy(QueryLogicGroupLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Negative limit given for query logic group 'TLD'");
        }

        /**
         * Verify that configurations with blank query logic patterns are forbidden.
         */
        @Test
        void testConfigWithBlankQueryLogicPattern() {
            givenConfig("TLD", " ", 50);

            assertThatThrownBy(QueryLogicGroupLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Blank query logic pattern given for query logic group 'TLD'");
        }

        /**
         * Verify that query logic patterns that cannot be compiled are forbidden.
         */
        @Test
        void testConfigWithUncompilableQueryLogicPattern() {
            givenConfig("TLD", "TLD[", 50);

            assertThatThrownBy(QueryLogicGroupLimitProviderTest.this::initProvider).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Invalid regex in query logic pattern 'TLD[' for query logic group 'TLD'");
        }
    }

    /**
     * Tests for {@link QueryLogicGroupLimitProvider#getBestGroupLimits(String)}.
     */
    @Nested
    class GetBestGroupLimitTests {
        /**
         * Verify that we support {@code *} as a wildcard pattern.
         */
        @Test
        void testImpliedWildcardQueryLogicPattern() {
            givenConfig("ImpliedWildcard", "*", 25);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("ImpliedWildcard", 25);
        }

        /**
         * Verify that an explicit wildcard pattern returns a limit for any query logic.
         */
        @Test
        void testExplicitWildcardQueryLogicPattern() {
            givenConfig("ExplicitWildcard", ".*", 25);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("ExplicitWildcard", 25);
        }

        /**
         * Verify that when there are only multiple wildcard matches, that the match with the lowest limit is returned.
         */
        @Test
        void testMultipleWildcardOnlyMatches() {
            givenConfig("HigherLimit", ".*", 25);
            givenConfig("LowerLimit", ".*", 10);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("LowerLimit", 10);
        }

        /**
         * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
         */
        @Test
        void testPartialMatchOverridesWildcardMatch() {
            givenConfig("Wildcard", ".*", 25);
            givenConfig("PartialMatch", "TLD.*", 40);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("PartialMatch", 40);
        }

        /**
         * Verify that when we have multiple partial matches, that all partial matches are returned in order of lowest limit to highest.
         */
        @Test
        void testMultiplePartialMatchesReturnsLowestLimit() {
            givenConfig("PartialMatch1", "TLD.*", 25);
            givenConfig("PartialMatch2", ".*TLD.*", 40);
            givenConfig("PartialMatch3", ".*LD.*", 15);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(3);
            assertThat(groupLimits).containsExactly(MapEntry.entry("PartialMatch3", 15), Map.entry("PartialMatch1", 25), Map.entry("PartialMatch2", 40));
        }

        /**
         * Verify that when we have an exact match, partial match and a wildcard match, that the exact match is returned.
         */
        @Test
        void testExactMatchOverridesPartialAndWildcardMatch() {
            givenConfig("Wildcard", ".*", 25);
            givenConfig("PartialMatch", "TLD.*", 40);
            givenConfig("ExactMatch", "TLDQueryLogic", 55);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("ExactMatch", 55);
        }

        /**
         * Verify that when we have multiple exact matches, that the one with the lowest limit is returned.
         */
        @Test
        void testMultipleExactMatchesReturnsLowestLimit() {
            givenConfig("ExactMatch1", "TLDQueryLogic", 25);
            givenConfig("ExactMatch2", "TLDQueryLogic", 40);
            givenConfig("ExactMatch3", "TLDQueryLogic", 55);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("TLDQueryLogic");
            assertThat(groupLimits).hasSize(1);
            assertThat(groupLimits).containsEntry("ExactMatch1", 25);
        }

        /**
         * Verify that when there is no match against a query logic group, that an empty map is returned.
         */
        @Test
        void testNoMatch() {
            givenConfig("PartialMatch", "TLD.*", 40);
            givenConfig("ExactMatch", "TLDQueryLogic", 55);

            initProvider();

            Map<String,Integer> groupLimits = provider.getBestGroupLimits("OtherQueryLogic");
            assertThat(groupLimits).isEmpty();
        }
    }

    /**
     * Tests for {@link QueryLogicGroupLimitProvider#createOverrides(Map, boolean)}.
     */
    @Nested
    class CreateOverridesTest {

        /**
         * Verify a map containing all default group limits are returned when an empty map is supplied and includeNonOverridden is true.
         */
        @Test
        void testNoOverridesAndIncludeNonOverridden() {
            givenConfig("Group1", "TLDQueryLogic", 25);
            givenConfig("Group2", "EventQueryLogic", 55);
            givenConfig("Group3", "EdgeQueryLigic", 30);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of(), true);
            assertThat(groupLimits).hasSize(3);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactly(
                                            tuple("Group1", 25),
                                            tuple("Group3", 30),
                                            tuple("Group2", 55));
            // @formatter:on
        }

        /**
         * Verify an empty map is returned when an empty map is supplied and includeNonOverridden is false.
         */
        @Test
        void testNoOverridesAndExcludeNonOverridden() {
            givenConfig("Group1", "TLDQueryLogic", 25);
            givenConfig("Group2", "EventQueryLogic", 55);
            givenConfig("Group3", "EdgeQueryLigic", 30);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of(), false);
            assertThat(groupLimits).isEmpty();
        }

        /**
         * Verify that when a non-empty map of overrides are passed in, and includeNonOverridden is false, a map containing overrides for the included groups
         * only is returned.
         */
        @Test
        void testGetOverridesAndExcludeNonOverridden() {
            givenConfig("Group1", "TLDQueryLogic", 25);
            givenConfig("Group2", "EventQueryLogic", 40);
            givenConfig("Group3", "EdgeQueryLigic", 55);
            givenConfig("Group4", "SuperQuery.*", 10);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of("Group2", 20, "Group4", 25), false);
            // @formatter:on
            assertThat(groupLimits).extracting("groupName", "queryLimit").containsExactly(tuple("Group2", 20), tuple("Group4", 25));
            // @formatter:on

        }

        /**
         * Verify that when a non-empty map of overrides are passed in, and includeNonOverridden is true, a map containing overrides for the included groups and
         * the non-overridden groups is returned.
         */
        @Test
        void testGetOverridesAndIncludeNonOverridden() {
            givenConfig("Group1", "TLDQueryLogic", 25);
            givenConfig("Group2", "EventQueryLogic", 40);
            givenConfig("Group3", "EdgeQueryLigic", 55);
            givenConfig("Group4", "SuperQuery.*", 10);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of("Group2", 20, "Group4", 25), true);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("Group2", 20),
                                            tuple("Group4", 25),
                                            tuple("Group1", 25),
                                            tuple("Group3", 55));
            // @formatter:on
        }

        /**
         * Verify a map containing all default group limits are returned when an empty map is supplied and includeNonOverridden is true, and that group limits
         * are not silently discarded if they have the same matcher type and equal limits.
         */
        @Test
        void testGroupsWithEqualMatcherTypeAndEqualDefaultLimits() {
            givenConfig("Group1", "TLDQueryLogic", 10);
            givenConfig("Group2", "EventQueryLogic", 10);
            givenConfig("Group3", "EdgeQueryLogic", 10);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of(), true);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("Group1", 10),
                                            tuple("Group3", 10),
                                            tuple("Group2", 10));
            // @formatter:on
        }

        /**
         * Verify a map containing all relevant group limits are returned when a non-empty map is supplied and includeNonOverridden is false, and that group
         * limits are not silently discarded if they have the same matcher type and equal limits.
         */
        @Test
        void testGroupsWithEqualMatcherTypeAndEqualOverriddenLimits() {
            givenConfig("Group1", "TLDQueryLogic", 45);
            givenConfig("Group2", "EventQueryLogic", 50);
            givenConfig("Group3", "EdgeQueryLogic", 25);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of("Group1", 10, "Group2", 10, "Group3", 10), false);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("Group1", 10),
                                            tuple("Group3", 10),
                                            tuple("Group2", 10));
            // @formatter:on
        }

        /**
         * Verify a map containing all relevant group limits are returned when a non-empty map is supplied and includeNonOverridden is true, and that group
         * limits are not silently discarded if they have the same matcher type and equal limits. Also verify non-overridden groups are included.
         */
        @Test
        void testGroupsWithEqualMatcherTypeAndEqualOverriddenLimitsAndIncludeNonOverridden() {
            givenConfig("Group1", "TLDQueryLogic", 45);
            givenConfig("Group2", "EventQueryLogic", 50);
            givenConfig("Group3", "EdgeQueryLogic", 25);

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(Map.of("Group1", 10, "Group2", 10), true);

            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("Group1", 10),
                                            tuple("Group3", 25),
                                            tuple("Group2", 10));
            // @formatter:on
        }

        /**
         * Verify that we obtain the correct group limits when we supply an override map with regex patterns against the group names.
         */
        @Test
        void testGroupsWithRegexPatternsWithoutWildcardAndIncludeNonOverridden() {
            givenConfig("GROUP_A_1", "TLDQueryLogic", 45);
            givenConfig("GROUP_A_2", "EventQueryLogic", 50);
            givenConfig("GROUP_A_3", "EdgeQueryLigic", 25);
            givenConfig("GROUP_B_1", "SuperQueryLogic.*", 15);
            givenConfig("GROUP_B_2", "EdgeQueryLogic", 5);
            givenConfig("GROUP_B_3", "EdgeQueryLogic", 10);
            givenConfig("GROUP_C_1", "EdgeQueryLogic", 30);
            givenConfig("GROUP_C_2", "EdgeQueryLogic", 25);

            // @formatter:off
            Map<String,Integer> overrideMap = Map.of(
                            "GROUP_A.*", 10, // We should get back all GROUP_A groups with a limit of 10.
                            "GROUP_B.*", 40, // We should get back all GROUP_B groups (except GROUP_B_3) with a limit of 40.
                            "GROUP_B_3", 50, // We should get back GROUP_B_3 with a limit of 50.
                            "GROUP_C_2", 10); // We should get back GROUP_C_2 with a limit of 10.
            // @formatter:on

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(overrideMap, true);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("GROUP_A_1", 10),
                                            tuple("GROUP_A_2", 10),
                                            tuple("GROUP_A_3", 10),
                                            tuple("GROUP_B_1", 40),
                                            tuple("GROUP_B_2", 40),
                                            tuple("GROUP_B_3", 50),
                                            tuple("GROUP_C_1", 30), // We should get back the original limit for GROUP_C_1.
                                            tuple("GROUP_C_2", 10));
            // @formatter:on
        }

        /**
         * Verify that a wildcard override will apply to all other remaining groups, even when includeOverridden is false.
         */
        @Test
        void testGroupsWithRegexPatternsWithWildcardAndIncludeNonOverridden() {
            givenConfig("GROUP_A_1", "TLDQueryLogic", 45);
            givenConfig("GROUP_A_2", "EventQueryLogic", 50);
            givenConfig("GROUP_A_3", "EdgeQueryLigic", 25);
            givenConfig("GROUP_B_1", "SuperQueryLogic.*", 15);
            givenConfig("GROUP_B_2", "EdgeQueryLogic", 5);
            givenConfig("GROUP_B_3", "EdgeQueryLogic", 10);
            givenConfig("GROUP_C_1", "EdgeQueryLogic", 30);
            givenConfig("GROUP_C_2", "EdgeQueryLogic", 25);

            // @formatter:off
            Map<String,Integer> overrideMap = Map.of(
                            "GROUP_A.*", 10, // We should get back all GROUP_A groups with a limit of 10.
                            "GROUP_B.*", 40, // We should get back all GROUP_B groups (except GROUP_B_3) with a limit of 40.
                            "GROUP_B_3", 50, // We should get back GROUP_B_3 with a limit of 50.
                            "*", 10); // We should get a limit of 10 for all remaining groups.
            // @formatter:on

            initProvider();

            SortedSet<QueryLogicGroupLimit> groupLimits = provider.createOverrides(overrideMap, false);
            // @formatter:off
            assertThat(groupLimits).extracting("groupName", "queryLimit")
                            .containsExactlyInAnyOrder(
                                            tuple("GROUP_A_1", 10),
                                            tuple("GROUP_A_2", 10),
                                            tuple("GROUP_A_3", 10),
                                            tuple("GROUP_B_1", 40),
                                            tuple("GROUP_B_2", 40),
                                            tuple("GROUP_B_3", 50),
                                            tuple("GROUP_C_1", 10),
                                            tuple("GROUP_C_2", 10));
            // @formatter:on
        }
    }

    private void initProvider() {
        provider = new QueryLogicGroupLimitProvider(200, configs);
    }

    private void givenConfig(String groupName, String queryLogicPattern, int queryLimit) {
        QueryLogicGroupLimitConfiguration config = new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit);
        configs.add(config);
    }
}

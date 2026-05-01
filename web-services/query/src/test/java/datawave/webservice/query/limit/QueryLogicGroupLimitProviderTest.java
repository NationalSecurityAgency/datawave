package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.AfterEach;
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

    private void initProvider() {
        provider = new QueryLogicGroupLimitProvider(200, configs);
    }

    private void givenConfig(String groupName, String queryLogicPattern, int queryLimit) {
        QueryLogicGroupLimitConfiguration config = new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit);
        configs.add(config);
    }
}

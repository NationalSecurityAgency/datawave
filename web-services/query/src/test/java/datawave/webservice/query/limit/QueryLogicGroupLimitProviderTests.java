package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Contains tests focused on validating behavior when retrieving limits for query logics.
 */
class QueryLogicGroupLimitProviderTests {

    private QueryLogicGroupLimitProvider provider;
    private final List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        provider = null;
        queryLogicGroupConfigs.clear();
    }

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

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Blank query logic pattern given for query logic group 'TLD'");
    }

    /**
     * Verify that query logic patterns that cannot be compiled are forbidden.
     */
    @Test
    void testConfigWithUncompilableQueryLogicPattern() {
        givenQueryLogicGroupConfig("TLD", "TLD[", 50);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Invalid regex in query logic pattern 'TLD[' for query logic group 'TLD'");
    }

    /**
     * Verify that we support {@code *} as a wildcard pattern.
     */
    @Test
    void testImpliedWildcardQueryLogicPattern() {
        givenQueryLogicGroupConfig("ImpliedWildcard", "*", 25);

        initProvider();

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("ImpliedWildcard", 25));
    }

    /**
     * Verify that an explicit wildcard pattern returns a limit for any query logic.
     */
    @Test
    void testExplicitWildcardQueryLogicPattern() {
        givenQueryLogicGroupConfig("ExplicitWildcard", ".*", 25);

        initProvider();

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("ExplicitWildcard", 25));
    }

    /**
     * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
     */
    @Test
    void testPartialMatchOverridesWildcardMatch() {
        givenQueryLogicGroupConfig("Wildcard", ".*", 25);
        givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);

        initProvider();

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("PartialMatch", 40));
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

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("PartialMatch3", 15));
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

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("ExactMatch", 55));
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

        Optional<QueryLogicGroupQueryLimit> actual = provider.getLimit("TLDQueryLogic");
        assertThat(actual.isPresent()).isTrue();
        assertThat(actual.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("ExactMatch1", 25));
    }

    /**
     * Verify that when there is no match against a query logic group, that an empty optional is returned.
     */
    @Test
    void testNoMatch() {
        givenQueryLogicGroupConfig("PartialMatch", "TLD.*", 40);
        givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 55);

        initProvider();

        Optional<QueryLogicGroupQueryLimit> optional = provider.getLimit("TLDQueryLogic");
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

        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("ExactMatch", 100));

        Optional<QueryLogicGroupQueryLimit> optional = provider.getOverriddenLimit("SYSTEM_01", "OtherQueryLogic", groupLimits);
        assertThat(optional.isPresent()).isFalse();
    }

    /**
     * Verify that when there are multiple matches for overridden limits, that the best match (that was present in the map of overridden limits) with the lowest
     * limit is returned.
     */
    @Test
    void testOverriddenPartialMatchesWithNonOverriddenExactMatch() {
        givenQueryLogicGroupConfig("PartialMatch01", "TLD.*", 200);
        givenQueryLogicGroupConfig("PartialMatch02", "TL.*", 75);
        givenQueryLogicGroupConfig("PartialMatch03", "TLDQuery[Ll]ogic", 300);
        givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 5);

        initProvider();

        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("PartialMatch01", 50));
        groupLimits.add(new MatchableLimit("PartialMatch03", 20));
        Optional<QueryLogicGroupQueryLimit> optional = provider.getOverriddenLimit("SYSTEM_01", "TLDQueryLogic", groupLimits);
        assertThat(optional.isPresent()).isTrue();

        // We should not receive the group ExactMatch, since we are filtering our results by the groups present in the map of overridden limits.
        assertThat(optional.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("PartialMatch03", 20));
    }

    /**
     * Verify that when there are multiple matches for overridden limits, that the best match (that was present in the map of overridden limits) with the lowest
     * limit is returned.
     */
    @Test
    void testOverriddenPartialMatchesWithOverriddenExactMatch() {
        givenQueryLogicGroupConfig("PartialMatch01", "TLD.*", 200);
        givenQueryLogicGroupConfig("PartialMatch02", "TL.*", 75);
        givenQueryLogicGroupConfig("PartialMatch03", "TLDQuery[Ll]ogic", 300);
        givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 5);

        initProvider();

        SortedSet<MatchableLimit> matchableLimits = new TreeSet<>();
        matchableLimits.add(new MatchableLimit("ExactMatch", 200));
        matchableLimits.add(new MatchableLimit("PartialMatch.*", 20));
        Optional<QueryLogicGroupQueryLimit> optional = provider.getOverriddenLimit("SYSTEM_01", "TLDQueryLogic", matchableLimits);
        assertThat(optional.isPresent()).isTrue();

        // We should receive ExactMatch this time, since it was present in the map of overridden limits.
        assertThat(optional.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("ExactMatch", 200));
    }

    /**
     * Verify that when there are multiple matches for overridden limits, that the best match (that was present in the map of overridden limits) with the lowest
     * limit is returned.
     */
    @Test
    void testOverriddenPartialMatchesWithOverriddenPartialMatch() {
        givenQueryLogicGroupConfig("TLD_APPLE", "TLD.*", 200);
        givenQueryLogicGroupConfig("TLD_APPLY", "TL.*", 75);
        givenQueryLogicGroupConfig("TLD_ABLY", "TLDQuery[Ll]ogic", 300);
        givenQueryLogicGroupConfig("ExactMatch", "TLDQueryLogic", 5);

        initProvider();

        SortedSet<MatchableLimit> matchableLimits = new TreeSet<>();
        matchableLimits.add(new MatchableLimit(".*", 10));
        matchableLimits.add(new MatchableLimit("TLD_.*", 20));
        Optional<QueryLogicGroupQueryLimit> optional = provider.getOverriddenLimit("SYSTEM_01", "TLDQueryLogic", matchableLimits);
        assertThat(optional.isPresent()).isTrue();

        // We should receive ExactMatch this time, since it was present in the map of overridden limits.
        assertThat(optional.get()).isEqualTo(QueryLogicGroupQueryLimit.fromConfig("TLD_.*", 20));
    }

    private void initProvider() {
        provider = new QueryLogicGroupLimitProvider(queryLogicGroupConfigs);
    }

    private void givenQueryLogicGroupConfig(String groupName, String queryLogicPattern, int queryLimit) {
        QueryLogicGroupLimitConfiguration config = new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit);
        queryLogicGroupConfigs.add(config);
    }
}

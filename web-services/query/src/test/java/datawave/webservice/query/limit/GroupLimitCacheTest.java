package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

class GroupLimitCacheTest {

    @Test
    void testNullLimits() {
        GroupLimitCache cache = GroupLimitCache.of(null, 200);
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void testEmptyLimits() {
        GroupLimitCache cache = GroupLimitCache.of(new TreeSet<>(), 200);
        assertThat(cache.isEmpty()).isTrue();
    }

    @Test
    void testNonEmptyLimits() {
        SortedSet<QueryLogicGroupLimit> limits = new TreeSet<>();

        // We should only receive this when there is no match to any other group.
        limits.add(new QueryLogicGroupLimit("Default_Wildcard", Matcher.getMatcher(".*", 200), 50));

        // We should receive all of these since they are better than the wildcard match, but have no exact match to take precedence.
        limits.add(new QueryLogicGroupLimit("Event_Partial_1", Matcher.getMatcher("Event.*", 200), 50));
        limits.add(new QueryLogicGroupLimit("Event_Partial_2", Matcher.getMatcher("EventQuery.*", 200), 25));
        limits.add(new QueryLogicGroupLimit("Event_Partial_3", Matcher.getMatcher("Eve.*", 200), 5));

        // We should receive only TLD_Exact_2 as the exact match with the lowest limit.
        limits.add(new QueryLogicGroupLimit("TLD_Partial_1", Matcher.getMatcher("TLD.*", 200), 30));
        limits.add(new QueryLogicGroupLimit("TLD_Partial_2", Matcher.getMatcher("TL.*", 200), 20));
        limits.add(new QueryLogicGroupLimit("TLD_Partial_3", Matcher.getMatcher("TLDQ.*", 200), 10));
        limits.add(new QueryLogicGroupLimit("TLD_Exact_1", Matcher.getMatcher("TLDQueryLogic", 200), 25));
        limits.add(new QueryLogicGroupLimit("TLD_Exact_2", Matcher.getMatcher("TLDQueryLogic", 200), 5));

        GroupLimitCache cache = GroupLimitCache.of(limits, 200);
        assertThat(cache.isEmpty()).isFalse();

        // Verify we got the exact match with the lowest limit for TLDQueryLogic.
        assertThat(cache.getGroupLimits("TLDQueryLogic")).containsExactly(Map.entry("TLD_Exact_2", 5));

        // Verify we got all the partial matches for EventQueryLogic, in order of lowest limit to highest.
        assertThat(cache.getGroupLimits("EventQueryLogic")).containsExactly(Map.entry("Event_Partial_3", 5), Map.entry("Event_Partial_2", 25),
                        Map.entry("Event_Partial_1", 50));

        // Verify we got the wildcard match for anything else.
        assertThat(cache.getGroupLimits("OtherQueryLogic")).containsExactly(Map.entry("Default_Wildcard", 50));
    }

    @Test
    void testNoMatch() {
        SortedSet<QueryLogicGroupLimit> limits = new TreeSet<>();
        limits.add(new QueryLogicGroupLimit("TLD", Matcher.getMatcher("TLD.*", 200), 50));

        GroupLimitCache cache = GroupLimitCache.of(limits, 200);
        assertThat(cache.isEmpty()).isFalse();

        // Verify we got an empty map due to no match against a group.
        assertThat(cache.getGroupLimits("EventQueryLogic")).isEmpty();
    }
}

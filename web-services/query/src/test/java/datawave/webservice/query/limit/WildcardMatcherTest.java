package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class WildcardMatcherTest {

    /**
     * Verify that {@link WildcardMatcher#matches(String)} always returns true.
     */
    @Test
    void testMatches() {
        WildcardMatcher matcher = new WildcardMatcher();
        assertThat(matcher.matches("anything")).isTrue();
        assertThat(matcher.matches("should")).isTrue();
        assertThat(matcher.matches("match")).isTrue();
    }

    /**
     * Verify that {@link WildcardMatcher#matchesAnyOf(Collection)} always returns true.
     */
    @Test
    void testMatchesAnyOf() {
        WildcardMatcher matcher = new WildcardMatcher();
        assertThat(matcher.matchesAnyOf(Set.of("anything", "should", "match"))).isTrue();
    }

    /**
     * Verify that {@link WildcardMatcher#getMatches(Collection)} returns a Set that is a copy of the given collection.
     */
    @Test
    void testGetMatches() {
        WildcardMatcher matcher = new WildcardMatcher();
        Set<String> matches = matcher.getMatches(List.of("anything", "anything", "should", "match"));
        assertThat(matches).containsExactlyInAnyOrder("anything", "should", "match");
    }
}

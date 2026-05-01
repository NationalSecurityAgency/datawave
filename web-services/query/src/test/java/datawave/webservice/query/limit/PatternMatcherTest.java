package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

class PatternMatcherTest {

    /**
     * Verify that null patterns are not allowed.
     */
    @Test
    void testNullPattern() {
        assertThatThrownBy(() -> new PatternMatcher(null, 200)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testGetPattern() {
        PatternMatcher patternMatcher = new PatternMatcher(Pattern.compile("TLD.*"), 200);
        assertThat(patternMatcher.getPattern().pattern()).isEqualTo("TLD.*");
    }

    /**
     * Verify that {@link PatternMatcher#matches(String)} returns true for a match.
     */
    @Test
    void testMatchesWithMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.matches("abc")).isTrue();
    }

    /**
     * Verify that {@link PatternMatcher#matches(String)} returns false for a non-match.
     */
    @Test
    void testMatchesWithNonMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.matches("efj")).isFalse();
    }

    /**
     * Verify that {@link PatternMatcher#matchesAnyOf(Collection)} returns true for a match.
     */
    @Test
    void testMatchesAnyOfWithMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.matchesAnyOf(Set.of("abc", "def", "ghi"))).isTrue();
    }

    /**
     * Verify that {@link PatternMatcher#matchesAnyOf(Collection)} returns false for a non-match.
     */
    @Test
    void testMatchesAnyOfWithNoMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.matchesAnyOf(Set.of("def", "ghi", "jkl"))).isFalse();
    }

    /**
     * Verify that {@link PatternMatcher#getMatches(Collection)} returns a populated set for a match.
     */
    @Test
    void testGetMatchesWithMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.getMatches(Set.of("abc", "def", "ghi", "ab", "abcde"))).containsExactlyInAnyOrder("abc", "ab", "abcde");
    }

    /**
     * Verify that {@link PatternMatcher#getMatches(Collection)} returns an empty set for a non-match.
     */
    @Test
    void testGetMatchesWithNoMatch() {
        PatternMatcher matcher = new PatternMatcher(Pattern.compile("ab.*"), 200);
        assertThat(matcher.getMatches(Set.of("def", "ghi", "jkl"))).isEmpty();
    }
}

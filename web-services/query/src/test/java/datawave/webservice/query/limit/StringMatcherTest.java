package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.Set;

import org.junit.jupiter.api.Test;

class StringMatcherTest {

    /**
     * Verify {@link StringMatcher} cannot be initialized with a null string.
     */
    @Test
    void testNullValue() {
        assertThatThrownBy(() -> new StringMatcher(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("value must not be null");
    }

    @Test
    void testGetValue() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.getValue()).isEqualTo("abc");
    }

    /**
     * Verify that {@link StringMatcher#matches(String)} returns true for a match.
     */
    @Test
    void testMatchesWithMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matches("abc")).isTrue();
    }

    /**
     * Verify that {@link StringMatcher#matches(String)} returns false for a non-match.
     */
    @Test
    void testMatchesWithNonMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matches("abcdef")).isFalse();
    }

    /**
     * Verify that {@link StringMatcher#matchesAnyOf(Collection)} returns true for a match.
     */
    @Test
    void testMatchesAnyOfWithMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matchesAnyOf(Set.of("abc", "def", "ghi"))).isTrue();
    }

    /**
     * Verify that {@link StringMatcher#matchesAnyOf(Collection)} returns false for a non-match.
     */
    @Test
    void testMatchesAnyOfWithNoMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matchesAnyOf(Set.of("def", "ghi", "jkl"))).isFalse();
    }

    /**
     * Verify that {@link StringMatcher#getMatches(Collection)} returns a populated set for a match.
     */
    @Test
    void testGetMatchesWithMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.getMatches(Set.of("abc", "def", "ghi"))).containsExactly("abc");
    }

    /**
     * Verify that {@link StringMatcher#getMatches(Collection)} returns an empty set for a non-match.
     */
    @Test
    void testGetMatchesWithNoMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.getMatches(Set.of("def", "ghi", "jkl"))).isEmpty();
    }
}

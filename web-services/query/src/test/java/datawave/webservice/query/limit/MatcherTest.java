package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MatcherTest {

    /**
     * Verify that {@link Matcher#getMatcher(String)} throws an exception when given a null string.
     */
    @Test
    void testGetMatcherGivenNull() {
        assertThatThrownBy(() -> Matcher.getMatcher(null)).isInstanceOf(NullPointerException.class);
    }

    /**
     * Verify that {@link Matcher#getMatcher(String)} returns a {@link StringMatcher} when given a non-regex string.
     */
    @Test
    void testGetMatcherGivenNonRegexString() {
        Matcher matcher = Matcher.getMatcher("abc");
        assertThat(matcher).isEqualTo(new StringMatcher("abc"));
    }

    /**
     * Verify that {@link Matcher#getMatcher(String)} returns a {@link PatternMatcher} when given a regex that does not consist solely of literals and escaped
     * literals.
     */
    @Test
    void testGetMatchGivenPattern() {
        Matcher matcher = Matcher.getMatcher("TLD.*");
        assertThat(matcher).isInstanceOf(PatternMatcher.class);
        PatternMatcher patternMatcher = (PatternMatcher) matcher;
        assertThat(patternMatcher.getPattern().pattern()).isEqualTo("TLD.*");
    }

    /**
     * Verify that {@link Matcher#getMatcher(String)} returns a {@link StringMatcher} when given a regex that consists solely of literals and escaped literals.
     */
    @Test
    void testGetMatchGivenPatternThatConsistsSolelyOfLiteralsAndEscapedLiterals() {
        Matcher matcher = Matcher.getMatcher("TLD\\g\\k");
        assertThat(matcher).isEqualTo(new StringMatcher("TLDgk"));
    }

    /**
     * Verify that {@link Matcher#getMatcher(String)} returns a {@link WildcardMatcher} when given the implied wildcard {@code *}.
     */
    @Test
    void testGetMatchGivenImpliedWildcard() {
        Matcher matcher = Matcher.getMatcher("*");
        assertThat(matcher).isEqualTo(WildcardMatcher.of());
    }

    /**
     * Verify that {@link Matcher#getMatcher(String)} returns a {@link WildcardMatcher} when given the explicit wildcard {@code .*}.
     */
    @Test
    void testGetMatchGivenExplicitWildcard() {
        Matcher matcher = Matcher.getMatcher(".*");
        assertThat(matcher).isEqualTo(WildcardMatcher.of());
    }
}

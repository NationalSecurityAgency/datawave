package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

class PatternMatcherTest {

    /**
     * Verify that null patterns are not allowed.
     */
    @Test
    void testNullPattern() {
        assertThatThrownBy(() -> new PatternMatcher(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("pattern must not be null");
    }

    /**
     * Verify that a matching string returns true.
     */
    @Test
    void testMatchingString() {
        PatternMatcher patternMatcher = new PatternMatcher(Pattern.compile("TLD.*"));
        assertThat(patternMatcher.matches("TLDQueryLogic")).isTrue();
    }

    /**
     * Verify that a non-matching string returns false.
     */
    @Test
    void testNonMatchingString() {
        PatternMatcher patternMatcher = new PatternMatcher(Pattern.compile("TLD.*"));
        assertThat(patternMatcher.matches("OtherQueryLogic")).isFalse();
    }
}

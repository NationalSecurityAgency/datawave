package datawave.webservice.query.limit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringMatcherTest {
    
    /**
     * Verify that a null string is forbidden.
     */
    @Test
    void testNullValue() {
        assertThatThrownBy(() -> new StringMatcher(null)).isInstanceOf(NullPointerException.class).hasMessageContaining("value must not be null");
    }
    
    @Test
    void testNonMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matches("abcdef")).isFalse();
    }
    
    @Test
    void testMatch() {
        StringMatcher matcher = new StringMatcher("abc");
        assertThat(matcher.matches("abc")).isTrue();
    }
}

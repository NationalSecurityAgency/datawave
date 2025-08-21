package datawave.webservice.query.limit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WildcardMatcherTest {
    
    /**
     * Verify that the wildcard matcher always returns true.
     */
    @Test
    void testMatchingBehavior() {
        WildcardMatcher matcher = new WildcardMatcher();
        assertThat(matcher.matches("anything")).isTrue();
    }
}

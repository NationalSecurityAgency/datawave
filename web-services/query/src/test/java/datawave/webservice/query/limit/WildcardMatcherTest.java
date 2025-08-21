package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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

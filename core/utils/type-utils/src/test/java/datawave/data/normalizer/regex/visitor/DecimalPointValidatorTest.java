package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.RegexParser.parse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DecimalPointValidatorTest {
    
    /**
     * Verify that validating a null node does not result in an exception.
     */
    @Test
    void testNullNode() {
        assertValid(null);
    }
    
    /**
     * Verify that validating an empty regex does not result in an exception.
     */
    @Test
    void testEmptyRegex() {
        assertValid("");
    }
    
    /**
     * Verify that validating sub-expressions with one decimal point does not result in exceptions.
     */
    @Test
    void testSingleDecimalPoints() {
        assertValid("23\\.3");
        assertValid("23\\.3|34\\.343");
    }
    
    /**
     * Verify that validating sub-expressions with more than one decimal point results in exceptions.
     */
    @Test
    void testMultipleDecimalPoints() {
        assertInvalid("34\\.34\\.3");
        assertInvalid("333|.*\\.43\\.34");
    }
    
    /**
     * Verify an alternations with valid combos do not result in an exception.
     */
    @Test
    void testValidAlternations() {
        assertValid("343|65\\.34|45\\.343.*");
    }
    
    private void assertValid(String pattern) {
        validate(pattern);
    }
    
    private void assertInvalid(String pattern) {
        assertThatThrownBy(() -> validate(pattern)).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Regex may not contain expressions with than one decimal point.");
    }
    
    private void validate(String pattern) {
        DecimalPointValidator.validate(parse(pattern));
    }
}

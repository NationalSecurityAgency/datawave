package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.RegexParser.parse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.visitor.NumericCharClassValidator;

class NumericCharClassValidatorTest {
    
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
     * Verify that validating regexes without character classes do not result in exceptions.
     */
    @Test
    void testRegexWithoutCharacterClass() {
        assertValid("123.*");
        assertValid("123.*{34}");
        assertValid("(234|34534)|343.*343.?");
    }
    
    /**
     * Verify that validating regexes with valid character classes do not result in exceptions.
     */
    @Test
    void testRegexWithValidCharacterClass() {
        // Allow all digits (including negated).
        assertValid("[123456789]");
        assertValid("[^123456789]");
        
        // Allow numeric ranges (including negated).
        assertValid("[0-9]");
        assertValid("[^0-9]");
        
        // Allow combinations (including negated).
        assertValid("[137-9]");
        assertValid("[^137-9]");
    }
    
    /**
     * Verify that validating regexes with invalid character classes results in exceptions.
     */
    @Test
    void testInvalidRegexes() {
        // Do not allow periods.
        assertInvalid("[.]");
        assertInvalid("[^.]");
        assertInvalid("[123.]");
        
        // Do not allow letter ranges.
        assertInvalid("[a-z]");
        assertInvalid("[A-Z]");
    }
    
    private void assertValid(String pattern) {
        validate(parse(pattern));
    }
    
    private void assertInvalid(String pattern) {
        assertThatThrownBy(() -> validate(parse(pattern))).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Character classes may only contain numeric characters and numeric ranges.");
    }
    
    private void validate(Node node) {
        NumericCharClassValidator.validate(node);
    }
}

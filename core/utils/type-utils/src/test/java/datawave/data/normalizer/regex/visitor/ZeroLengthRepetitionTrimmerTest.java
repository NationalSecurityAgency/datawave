package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class ZeroLengthRepetitionTrimmerTest {
    
    @Test
    void testNullNode() {
        assertNotTrimmed(null);
    }
    
    @Test
    void testEmptyRegex() {
        assertNotTrimmed("");
    }
    
    @Test
    void testRegexWithoutRepetitions() {
        assertNotTrimmed("123.*");
        assertNotTrimmed("(234|34534)|343.*343.?");
    }
    
    @Test
    void testRegexWithValidRepetitions() {
        // Any any non-zero combination.
        assertNotTrimmed("2{1}");
        assertNotTrimmed("2{12}");
        assertNotTrimmed("2{1,6}");
        assertNotTrimmed("2{10,20}");
        
        // Allow {0,} as an equivalent to *.
        assertNotTrimmed("2{0,}");
    }
    
    @Test
    void testInvalidRegexes() {
        assertTrimmedTo("2{0}", null);
        assertTrimmedTo("2{0,0}", null);
        assertTrimmedTo("3{0,0}|[4-6]{0}", null);
        assertTrimmedTo("23.*5{0}", "23.*");
        assertTrimmedTo("23.*5{0,0}", "23.*");
        assertTrimmedTo("23.*5{0,0}|65{3}", "23.*|65{3}");
    }
    
    private void assertNotTrimmed(String pattern) {
        assertTrimmedTo(pattern, pattern);
    }
    
    private void assertTrimmedTo(String pattern, String expectedPattern) {
        Node actual = ZeroLengthRepetitionTrimmer.trim(parse(pattern));
        if (expectedPattern == null) {
            PrintVisitor.printToSysOut(actual);
            assertThat(actual).isNull();
        } else {
            Node expected = parse(expectedPattern);
            assertThat(actual).isEqualTreeTo(expected);
        }
    }
}

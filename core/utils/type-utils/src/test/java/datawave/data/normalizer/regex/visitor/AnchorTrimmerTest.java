package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.visitor.AnchorTrimmer;

class AnchorTrimmerTest {
    
    @Test
    void testNullNode() {
        assertNotTrimmed(null);
    }
    
    @Test
    void testEmptyNode() {
        assertNotTrimmed("");
    }
    
    @Test
    void testRegexesWithoutAnchors() {
        assertNotTrimmed("123.*");
        assertNotTrimmed("(234[0-9]|342).*");
        assertNotTrimmed(".*234\\d\\.3{3}");
    }
    
    @Test
    void testRegexesWithAnchors() {
        assertTrimmedTo("^123.*$", "123.*");
        assertTrimmedTo("^123.*$|^65[0-9]{3}$", "123.*|65[0-9]{3}");
        assertTrimmedTo("(^123.*$)|(^65[0-9]{3}$)", "(123.*)|(65[0-9]{3})");
        assertTrimmedTo("(^123.*|65[0-9]{3}$)", "(123.*|65[0-9]{3})");
        assertTrimmedTo("^123.*|65[0-9]{3}$", "123.*|65[0-9]{3}");
    }
    
    private void assertNotTrimmed(String pattern) {
        assertTrimmedTo(pattern, pattern);
    }
    
    private void assertTrimmedTo(String pattern, String expectedPattern) {
        Node actual = AnchorTrimmer.trim(parse(pattern));
        if (expectedPattern == null) {
            assertThat(actual).isNull();
        } else {
            Node expected = parse(expectedPattern);
            assertThat(actual).isEqualTreeTo(expected);
        }
    }
}

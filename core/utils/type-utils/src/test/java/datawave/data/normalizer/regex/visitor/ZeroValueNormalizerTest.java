package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;

class ZeroValueNormalizerTest {
    
    /**
     * Test different variants of zero and negative zero. These can be handled by {@link datawave.data.type.util.NumericalEncoder#encode(String)} and do not
     * need to be changed.
     */
    @Test
    public void testSimpleNumberZeros() {
        assertNotExpanded("0");
        assertNotExpanded("0\\.00");
        assertNotExpanded("-0");
        assertNotExpanded("-0\\.00");
    }
    
    @Test
    void testPositivePatternsThatCanMatchZero() {
        assertExpanded("0.*", "0.*|0");
        assertExpanded(".*0", ".*0|0");
        assertExpanded(".+0", ".+0|0");
        assertExpanded("[0-9]", "[0-9]|0");
        assertExpanded(".*0.*", ".*0.*|0");
        assertExpanded("0.", "0.|0");
        assertExpanded("0\\d", "0\\d|0");
        assertExpanded("\\d", "\\d|0");
    }
    
    @Test
    void testPositivePatternsThatOnlyMatchZero() {
        assertExpanded("0\\.0[0]", "0");
        assertExpanded("0\\.0[0-0]", "0");
    }
    
    @Test
    void testNegativePatternsThatOnlyMatchZero() {
        assertExpanded("-0\\.0[0]", "0");
        assertExpanded("-0\\.0[0-0]", "0");
    }
    
    @Test
    void testNegativePatternsThatCanMatchZero() {
        assertExpanded("-[01234]", "-[01234]|0");
        assertExpanded("-[0-9]", "-[0-9]|0");
        assertExpanded("-\\d", "-\\d|0");
        assertExpanded("-.", "-.|0");
        assertExpanded("-.*", "-.*|0");
        assertExpanded("-.+", "-.+|0");
        assertExpanded("-0.00.*", "-0.00.*|0");
        assertExpanded("-0.00.*", "-0.00.*|0");
        assertExpanded("-0.00\\d", "-0.00\\d|0");
        assertExpanded("-00\\.0\\d.", "-00\\.0\\d.|0");
        assertExpanded("-[0-3]0\\d.", "-[0-3]0\\d.|0");
    }
    
    @Test
    void testNegativePatternsThatCannotMatchZero() {
        assertNotExpanded("-234[0-3]");
        assertNotExpanded("-.*834");
        assertNotExpanded("-0\\.00.*834");
        assertNotExpanded("-.00.001");
    }
    
    @Test
    void testAlternations() {
        assertExpanded("0\\.93|34.*|-34.*|0\\.0[0]|-0.00\\d", "0\\.93|34.*|-34.*|0|-0.00\\d|0");
    }
    
    public void assertNotExpanded(String pattern) {
        assertExpanded(pattern, pattern);
    }
    
    public void assertExpanded(String pattern, String expectedPattern) {
        Node actual = ZeroValueNormalizer.expand(parse(pattern));
        Node expected = parse(expectedPattern);
        assertThat(actual).isEqualTreeTo(expected);
    }
}

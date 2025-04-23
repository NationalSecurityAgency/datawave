package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexParser;

class SimpleNumberEncoderTest {
    
    @Test
    void testEmpty() {
        assertNotEncoded("");
    }
    
    @Test
    void testPatternsWithoutSimpleNumbers() {
        assertNotEncoded("12.*");
        assertNotEncoded("12[3-5]");
        assertNotEncoded("12{3}");
        assertNotEncoded("12\\d");
        assertNotEncoded("12.");
        assertNotEncoded("12+");
        assertNotEncoded("12?");
        assertNotEncoded("(12).");
        assertNotEncoded("12.?|.*45|43.*");
    }
    
    @Test
    void testSinglePositiveSimpleNumber() {
        Node actual = assertEncoded("123\\.45", "\\+cE1\\.2345");
        // Verify an encoded number node was returned.
        assertThat(actual).isEncodedNumberNode();
    }
    
    @Test
    void testSingleNegativeNumber() {
        Node actual = assertEncoded("-342", "!XE6\\.58");
        // Verify an encoded number node was returned.
        assertThat(actual).isEncodedNumberNode();
    }
    
    @Test
    void testAlternatedPositiveSimpleNumberAndNonSimpleNumber() {
        Node actual = assertEncoded("-342|23.*", "!XE6\\.58|23.*");
        // Verify that the alternation node has an encoded number node (0) and an expression node (1) as children.
        assertThat(actual).assertChild(0).isAlternationNode().assertChild(0).isEncodedNumberNode().assertParent().assertChild(1).isExpressionNode();
    }
    
    @Test
    void testAlternatedNegativeSimpleNumberAndNonSimpleNumber() {
        Node actual = assertEncoded("-34.*|23", "-34.*|\\+bE2\\.3");
        // Verify that the alternation node has an expression node (0) and an encoded number node (0) as children.
        assertThat(actual).assertChild(0).isAlternationNode().assertChild(0).isExpressionNode().assertParent().assertChild(1).isEncodedNumberNode();
    }
    
    @Test
    void testAlternatedPositiveAndNegativeSimpleNumber() {
        Node actual = assertEncoded("5345|-4452", "\\+dE5\\.345|!WE5\\.548");
        // Verify that the alternation node has two encoded number nodes as children.
        assertThat(actual).assertChild(0).isAlternationNode().assertChild(0).isEncodedNumberNode().assertParent().assertChild(1).isEncodedNumberNode();
    }
    
    private void assertNotEncoded(String pattern) {
        assertEncoded(pattern, pattern);
    }
    
    private Node assertEncoded(String pattern, String expectedPattern) {
        Node actual = SimpleNumberEncoder.encode(RegexParser.parse(pattern));
        if (expectedPattern == null) {
            assertThat(actual).isNull();
        } else {
            assertThat(actual).asTreeString().isEqualTo(expectedPattern);
        }
        return actual;
    }
}

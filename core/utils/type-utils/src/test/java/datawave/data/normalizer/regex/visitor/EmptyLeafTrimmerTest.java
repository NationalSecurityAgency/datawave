package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.NodeAssert.assertThat;
import static datawave.data.normalizer.regex.RegexParser.parse;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.visitor.EmptyLeafTrimmer;

class EmptyLeafTrimmerTest {
    
    @Test
    void testTrimDoesNotModifyOriginal() {
        ExpressionNode original = parse("12|()||(45|3)");
        Node trimmed = EmptyLeafTrimmer.trim(original);
        
        assertThat(original).isEqualTreeTo(parse("12|()||(45|3)"));
        assertThat(trimmed).isEqualTreeTo(parse("12|(45|3)"));
    }
    
    @Test
    void testTrimmingTreeWithNoEmptyNodes() {
        assertNotTrimmed("1|3");
        assertNotTrimmed("(234)");
        assertNotTrimmed("(234)|546");
    }
    
    @Test
    void testTrimmingEmptyAlternations() {
        assertTrimmedTo("|3", "3");
        assertTrimmedTo("3||4||5", "3|4|5");
        assertTrimmedTo("3|", "3");
    }
    
    @Test
    void testTrimmingEmptyGroups() {
        assertTrimmedTo("()|(35)", "(35)");
        assertTrimmedTo("(2|5|())", "(2|5)");
    }
    
    @Test
    void testTrimmingRegexConsistingOfEmptyAlternationsAndGroups() {
        assertTrimmedTo("|()|()", null);
    }
    
    @Test
    void testTrimmingEmptyNode() {
        assertTrimmedTo("", null);
    }
    
    @Test
    void testTrimmingExpressionWithEmptyNode() {
        Node node = new ExpressionNode();
        node.addChild(new EmptyNode());
        assertThat(EmptyLeafTrimmer.trim(node)).isNull();
    }
    
    private void assertNotTrimmed(String pattern) {
        assertTrimmedTo(pattern, pattern);
    }
    
    private void assertTrimmedTo(String pattern, String expectedPattern) {
        Node actual = EmptyLeafTrimmer.trim(parse(pattern));
        if (expectedPattern == null) {
            assertThat(actual).isNull();
        } else {
            Node expected = parse(expectedPattern);
            assertThat(actual).isEqualTreeTo(expected);
        }
    }
}

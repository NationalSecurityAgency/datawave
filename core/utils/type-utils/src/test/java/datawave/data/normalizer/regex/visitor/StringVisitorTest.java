package datawave.data.normalizer.regex.visitor;

import static datawave.data.normalizer.regex.RegexParser.parse;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexParser;
import datawave.data.normalizer.regex.visitor.StringVisitor;

class StringVisitorTest {
    
    @Test
    void testNullNode() {
        assertThat(toString(null)).isNull();
    }
    
    @Test
    void testEmptyExpression() {
        ExpressionNode node = new ExpressionNode();
        node.addChild(new EmptyNode());
        assertThat(toString(node)).isEqualTo("");
    }
    
    @Test
    void testComplexTrees() {
        assertThat(toString(parse("-234\\.3"))).isEqualTo("-234\\.3");
        assertThat(toString(parse("234.*"))).isEqualTo("234.*");
        assertThat(toString(parse("234[^65.]"))).isEqualTo("234[^65.]");
        assertThat(toString(parse("^2{3}.+"))).isEqualTo("^2{3}.+");
        assertThat(toString(parse("2{3,}.*"))).isEqualTo("2{3,}.*");
        assertThat(toString(parse("2{2,4}.*"))).isEqualTo("2{2,4}.*");
        assertThat(toString(parse("(23|65)"))).isEqualTo("(23|65)");
        assertThat(toString(parse("(23|65)|(34[65].*)"))).isEqualTo("(23|65)|(34[65].*)");
        assertThat(toString(parse("35\\d.+"))).isEqualTo("35\\d.+");
    }
    
    private String toString(Node node) {
        return StringVisitor.toString(node);
    }
}

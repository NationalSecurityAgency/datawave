package datawave.data.normalizer.regex.visitor;

import java.util.Iterator;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.AnyCharNode;
import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.DigitCharClassNode;
import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.EndAnchorNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.IntegerNode;
import datawave.data.normalizer.regex.IntegerRangeNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.StartAnchorNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

public class StringVisitor implements Visitor {
    
    public static String toString(Node node) {
        if (node == null) {
            return null;
        }
        StringVisitor visitor = new StringVisitor();
        StringBuilder sb = new StringBuilder();
        node.accept(visitor, sb);
        return sb.toString();
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        Iterator<Node> iterator = node.getChildren().iterator();
        while (iterator.hasNext()) {
            iterator.next().accept(this, sb);
            if (iterator.hasNext()) {
                sb.append("|");
            }
        }
        return null;
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("(");
        node.childrenAccept(this, sb);
        sb.append(")");
        return null;
    }
    
    @Override
    public Object visitDigitChar(DigitCharClassNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("\\d");
        return null;
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("[");
        if (node.isNegated()) {
            sb.append("^");
        }
        node.childrenAccept(this, sb);
        sb.append("]");
        return null;
    }
    
    @Override
    public Object visitCharRange(CharRangeNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.getStart()).append("-").append(node.getEnd());
        return null;
    }
    
    @Override
    public Object visitSingleChar(SingleCharNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.getCharacter());
        return null;
    }
    
    @Override
    public Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("\\").append(node.getCharacter());
        return null;
    }
    
    @Override
    public Object visitRepetition(RepetitionNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("{");
        node.childrenAccept(this, sb);
        sb.append("}");
        return null;
    }
    
    @Override
    public Object visitQuestionMark(QuestionMarkNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("?");
        return null;
    }
    
    @Override
    public Object visitAnyChar(AnyCharNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(".");
        return null;
    }
    
    @Override
    public Object visitZeroToMany(ZeroOrMoreNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("*");
        return null;
    }
    
    @Override
    public Object visitOneToMany(OneOrMoreNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("+");
        return null;
    }
    
    @Override
    public Object visitInteger(IntegerNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.getValue());
        return null;
    }
    
    @Override
    public Object visitIntegerRange(IntegerRangeNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.getStart());
        sb.append(",");
        if (node.isEndBounded()) {
            sb.append(node.getEnd());
        }
        return null;
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        return null;
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("^");
        return null;
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("$");
        return null;
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        node.childrenAccept(this, data);
        return null;
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        node.childrenAccept(this, data);
        return null;
    }
}

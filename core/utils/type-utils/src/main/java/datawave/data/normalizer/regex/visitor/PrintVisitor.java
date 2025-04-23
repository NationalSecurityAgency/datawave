package datawave.data.normalizer.regex.visitor;

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

/**
 * A {@link Visitor} implementation that accepts a {@link Node} tree and streams a pretty-print of it to {@link System#out}.
 */
public class PrintVisitor implements Visitor {
    
    private static final String PREFIX = "  ";
    
    private interface Output {
        void write(String line);
    }
    
    private static class SystemOutput implements Output {
        
        @Override
        public void write(String line) {
            System.out.println(line);
        }
    }
    
    private static class StringBuilderOutput implements Output {
        
        private final StringBuilder sb = new StringBuilder();
        
        @Override
        public void write(String line) {
            sb.append("\n").append(line);
        }
    }
    
    /**
     * Streams a pretty-print of the given node to {@link System#out}.
     * 
     * @param node
     *            the node to print
     */
    public static void printToSysOut(Node node) {
        if (node == null) {
            System.out.println("null");
        } else {
            PrintVisitor visitor = new PrintVisitor(new SystemOutput());
            node.accept(visitor, "");
        }
    }
    
    /**
     * Returns a string containing a pretty print of the given node.
     * 
     * @param node
     *            the node
     * @return the string
     */
    public static String printToString(Node node) {
        if (node == null) {
            return "null";
        } else {
            StringBuilderOutput output = new StringBuilderOutput();
            PrintVisitor visitor = new PrintVisitor(output);
            node.accept(visitor, "");
            return output.sb.toString();
        }
    }
    
    private final Output output;
    
    protected PrintVisitor(Output output) {
        this.output = output;
    }
    
    private void print(Node node, Object data) {
        printLine(node, data);
        if (node != null) {
            node.childrenAccept(this, (data + PREFIX));
        }
    }
    
    private void printLine(Node node, Object data) {
        output.write(data + "" + node);
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitGroup(GroupNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitDigitChar(DigitCharClassNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitCharClass(CharClassNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitCharRange(CharRangeNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitSingleChar(SingleCharNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitRepetition(RepetitionNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitQuestionMark(QuestionMarkNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitAnyChar(AnyCharNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitZeroToMany(ZeroOrMoreNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitOneToMany(OneOrMoreNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitInteger(IntegerNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitIntegerRange(IntegerRangeNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitEmpty(EmptyNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitEncodedNumber(EncodedNumberNode node, Object data) {
        print(node, data);
        return null;
    }
    
    @Override
    public Object visitEncodedPattern(EncodedPatternNode node, Object data) {
        print(node, data);
        return null;
    }
}

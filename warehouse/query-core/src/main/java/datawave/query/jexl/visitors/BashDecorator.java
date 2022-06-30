package datawave.query.jexl.visitors;

import java.util.Collection;

import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Decorates a Jexl Query with bash coloring. To be used in viewing Jexl Queries in bash.
 */
public class BashDecorator implements JexlQueryDecorator {
    
    protected static final String NC = "\\e[0m"; // No color
    protected static final String NOT_EQUAL_COLOR = "\\e[38;5;88m"; // dark red
    protected static final String OR_COLOR = "\\e[38;5;1m"; // light red
    protected static final String LESS_THAN_COLOR = "\\e[38;5;2m"; // dark green
    protected static final String NR_COLOR = "\\e[38;5;46m"; // light green
    protected static final String MARKER_COLOR = "\\e[38;5;108m"; // green blue
    protected static final String AND_COLOR = "\\e[38;5;21m"; // dark blue
    protected static final String STRING_COLOR = "\\e[38;5;33m"; // light blue
    protected static final String GREATER_THAN_COLOR = "\\e[38;5;69m"; // blue purple
    protected static final String FIELD_COLOR = "\\e[38;5;11m"; // light yellow
    protected static final String NULL_COLOR = "\\e[38;5;229m"; // yellow white
    protected static final String EQUAL_COLOR = "\\e[38;5;51m"; // cyan
    protected static final String NOT_COLOR = "\\e[38;5;162m"; // dark pink
    protected static final String FUNCTION_COLOR = "\\e[38;5;202m"; // dark orange
    protected static final String ER_COLOR = "\\e[38;5;214m"; // light orange
    protected static final String BOOLEAN_COLOR = "\\e[38;5;210m"; // orange pink
    protected static final String ASSIGN_COLOR = "\\e[38;5;201m"; // light pink
    protected static final String NUMBER_COLOR = "\\e[38;5;225m"; // pink white
    protected static final String METHOD_COLOR = "\\e[38;5;91m"; // dark purple
    protected static final String GREATER_EQUAL_COLOR = "\\e[38;5;129m"; // bright purple
    protected static final String NAMESPACE_COLOR = "\\e[38;5;135m"; // dull purple
    protected static final String LESS_EQUAL_COLOR = "\\e[38;5;94m"; // brown orange
    protected static final String ARITHMETIC_OP_COLOR = "\\e[38;5;10m"; // green teal
    
    @Override
    public void apply(StringBuilder sb, ASTOrNode node, Collection<String> childStrings) {
        sb.append(String.join(OR_COLOR + " || " + NC + NEWLINE, childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAndNode node, Collection<String> childStrings, boolean needNewLines) {
        sb.append(String.join(AND_COLOR + " && " + NC + (needNewLines ? NEWLINE : ""), childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTEQNode node) {
        sb.append(EQUAL_COLOR + " == " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNENode node) {
        sb.append(NOT_EQUAL_COLOR + " != " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLTNode node) {
        sb.append(LESS_THAN_COLOR + " < " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGTNode node) {
        sb.append(GREATER_THAN_COLOR + " > " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLENode node) {
        sb.append(LESS_EQUAL_COLOR + " <= " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGENode node) {
        sb.append(GREATER_EQUAL_COLOR + " >= " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTERNode node) {
        sb.append(ER_COLOR + " =~ " + NC);
        
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNRNode node) {
        sb.append(NR_COLOR + " !~ " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNotNode node) {
        sb.append(NOT_COLOR + "!" + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTIdentifier node) {
        // We want to remove the $ if present and only replace it when necessary
        String fieldName = JexlASTHelper.rebuildIdentifier(JexlASTHelper.deconstructIdentifier(node.image));
        // Go up the tree until reaching null or an AND node. This allows us to determine if this is a Query Property Marker
        JexlNode parent = node;
        while (parent != null && !(parent instanceof ASTAndNode)) {
            parent = parent.jjtGetParent();
        }
        if (QueryPropertyMarker.findInstance(parent).isAnyType())
            sb.append(MARKER_COLOR + fieldName + NC);
        else
            sb.append(FIELD_COLOR + fieldName + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNullLiteral node) {
        sb.append(NULL_COLOR + "null" + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTTrueNode node) {
        sb.append(BOOLEAN_COLOR + "true" + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFalseNode node) {
        sb.append(BOOLEAN_COLOR + "false" + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTStringLiteral node, String literal) {
        sb.append(STRING_COLOR).append('\'').append(literal).append('\'').append(NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFunctionNode node, int i) {
        if (i == 0)
            sb.append(NAMESPACE_COLOR);
        else if (i == 1)
            sb.append(FUNCTION_COLOR);
        // Color is changed back to normal (NC) on visiting the ASTIdentifier children
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMethodNode node, StringBuilder methodStringBuilder) {
        sb.append(METHOD_COLOR + methodStringBuilder.toString() + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNumberLiteral node) {
        sb.append(NUMBER_COLOR + node.image + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAdditiveOperator node) {
        sb.append(ARITHMETIC_OP_COLOR + node.image + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTSizeMethod node) {
        sb.append(METHOD_COLOR + ".size() " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMulNode node) {
        sb.append(ARITHMETIC_OP_COLOR + " * " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTDivNode node) {
        sb.append(ARITHMETIC_OP_COLOR + " / " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTModNode node) {
        sb.append(ARITHMETIC_OP_COLOR + " % " + NC);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAssignment node, int i) {
        String str = ASSIGN_COLOR + " = " + NC;
        sb.append(str);
        if (i + 1 == node.jjtGetNumChildren())
            sb.setLength(sb.length() - str.length());
    }
    
    @Override
    public void apply(StringBuilder sb, ASTUnaryMinusNode node) {
        sb.append(ARITHMETIC_OP_COLOR + "-" + NC);
    }
    
    @Override
    public void removeFieldColoring(StringBuilder sb) {
        int indexOfStr = sb.indexOf(BashDecorator.FIELD_COLOR);
        if (indexOfStr != -1) {
            sb.delete(indexOfStr, sb.indexOf("m", indexOfStr) + 1);
        }
    }
    
}

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

import datawave.query.jexl.JexlASTHelper;

/**
 * Does not add any decoration to the Jexl Query (no added colors)
 */
public class EmptyDecorator implements JexlQueryDecorator {
    @Override
    public void apply(StringBuilder sb, ASTAdditiveOperator node) {
        sb.append(node.image);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAndNode node, Collection<String> childStrings, boolean needNewLines) {
        sb.append(String.join(" && " + (needNewLines ? NEWLINE : ""), childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAssignment node, int i) {
        sb.append(" = ");
        if (i + 1 == node.jjtGetNumChildren())
            sb.setLength(sb.length() - " = ".length());
    }
    
    @Override
    public void apply(StringBuilder sb, ASTDivNode node) {
        sb.append(" / ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTEQNode node) {
        sb.append(" == ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTERNode node) {
        sb.append(" =~ ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFalseNode node) {
        sb.append("false");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFunctionNode node, int i) {
        ; // Nothing to do
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGENode node) {
        sb.append(" >= ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGTNode node) {
        sb.append(" > ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTIdentifier node) {
        // We want to remove the $ if present and only replace it when necessary
        String fieldName = JexlASTHelper.rebuildIdentifier(JexlASTHelper.deconstructIdentifier(node.image));
        
        sb.append(fieldName);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLENode node) {
        sb.append(" <= ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLTNode node) {
        sb.append(" < ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMethodNode node, StringBuilder methodStringBuilder) {
        sb.append(methodStringBuilder);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTModNode node) {
        sb.append(" % ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMulNode node) {
        sb.append(" * ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNENode node) {
        sb.append(" != ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNotNode node) {
        sb.append("!");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNRNode node) {
        sb.append(" !~ ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNullLiteral node) {
        sb.append("null");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNumberLiteral node) {
        sb.append(node.image);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTOrNode node, Collection<String> childStrings) {
        sb.append(String.join(" || " + NEWLINE, childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTSizeMethod node) {
        sb.append(".size() ");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTStringLiteral node, String literal) {
        sb.append('\'').append(literal).append('\'');
    }
    
    @Override
    public void apply(StringBuilder sb, ASTTrueNode node) {
        sb.append("true");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTUnaryMinusNode node) {
        sb.append("-");
    }
    
    @Override
    public void removeFieldColoring(StringBuilder sb) {
        ; // Nothing to do
    }
}

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
 * Decorates a Jexl Query with HTML coloring. To be used in viewing Jexl Queries on web pages. Specifically, the Query Metrics webpage.
 */
public class HtmlDecorator implements JexlQueryDecorator {
    
    @Override
    public void apply(StringBuilder sb, ASTAdditiveOperator node) {
        sb.append(createSpan("add-op", node.image));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAndNode node, Collection<String> childStrings, boolean needNewLines) {
        sb.append(String.join(SPACE + createSpan("and-op", "&&") + SPACE + (needNewLines ? NEWLINE : ""), childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAssignment node, int i) {
        String str = SPACE + createSpan("assign-op", "=") + SPACE;
        sb.append(str);
        if (i + 1 == node.jjtGetNumChildren())
            sb.setLength(sb.length() - str.length());
    }
    
    @Override
    public void apply(StringBuilder sb, ASTDivNode node) {
        sb.append(SPACE + createSpan("div-op", "/") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTEQNode node) {
        sb.append(SPACE + createSpan("equal-op", "==") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTERNode node) {
        sb.append(SPACE + createSpan("ER-op", "=~") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFalseNode node) {
        sb.append(createSpan("boolean-value", "false"));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFunctionNode node, int i) {
        if (i == 0)
            sb.append("<span class=\"function-namespace\">");
        else if (i == 1)
            sb.append("<span class=\"function\">");
        // the span elements are closed on visiting the ASTIdentifier children
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGENode node) {
        sb.append(SPACE + createSpan("greater-than-equal-op", ">=") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGTNode node) {
        sb.append(SPACE + createSpan("greater-than-op", ">") + SPACE);
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
            sb.append(createSpan("query-property-marker", fieldName));
        else
            sb.append(createSpan("field", fieldName));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLENode node) {
        sb.append(SPACE + createSpan("less-than-equal-op", "<=") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLTNode node) {
        sb.append(SPACE + createSpan("less-than-op", "<") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMethodNode node, StringBuilder methodStringBuilder) {
        sb.append(createSpan("method", methodStringBuilder.toString()));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTModNode node) {
        sb.append(SPACE + createSpan("mod-op", "%") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMulNode node) {
        sb.append(SPACE + createSpan("mul-op", "*") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNENode node) {
        sb.append(SPACE + createSpan("not-equal-op", "!=") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNotNode node) {
        sb.append(createSpan("not-op", "!"));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNRNode node) {
        sb.append(SPACE + createSpan("NR-op", "!~") + SPACE);
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNullLiteral node) {
        sb.append(createSpan("null-value", "null"));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNumberLiteral node) {
        sb.append(createSpan("numeric-value", node.image));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTOrNode node, Collection<String> childStrings, boolean needNewLines) {
        sb.append(String.join(SPACE + createSpan("or-op", "||") + SPACE + (needNewLines ? NEWLINE : ""), childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTSizeMethod node) {
        sb.append(createSpan("method", ".size()"));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTStringLiteral node, String literal) {
        sb.append(createSpan("string-value", SINGLE_QUOTE + literal + SINGLE_QUOTE));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTTrueNode node) {
        sb.append(createSpan("boolean-value", "true"));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTUnaryMinusNode node) {
        sb.append(createSpan("unary-minus", "-"));
    }
    
    private String createSpan(String cssClass, String content) {
        return String.format("<span class=\"%s\">%s</span>", cssClass, content);
    }
    
    @Override
    public void removeFieldColoring(StringBuilder sb) {
        String strToRem = "<span class=\"field\">";
        
        int indexOfStr = sb.indexOf(strToRem);
        if (indexOfStr != -1) {
            sb.delete(indexOfStr, indexOfStr + strToRem.length());
        }
    }
}

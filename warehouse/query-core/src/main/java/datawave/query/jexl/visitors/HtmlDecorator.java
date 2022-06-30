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
    public void apply(StringBuilder sb, ASTOrNode node, Collection<String> childStrings) {
        sb.append(String.join("<span class=\"or-op\"> || </span>" + NEWLINE, childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAndNode node, Collection<String> childStrings, boolean needNewLines) {
        sb.append(String.join("<span class=\"and-op\"> && </span>" + (needNewLines ? NEWLINE : ""), childStrings));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTEQNode node) {
        sb.append("<span class=\"equal-op\"> == </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNENode node) {
        sb.append("<span class=\"not-equal-op\"> != </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLTNode node) {
        sb.append("<span class=\"less-than-op\"> < </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGTNode node) {
        sb.append("<span class=\"greater-than-op\"> > </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTLENode node) {
        sb.append("<span class=\"less-than-equal-op\"> <= </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTGENode node) {
        sb.append("<span class=\"greater-than-equal-op\"> >= </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTERNode node) {
        sb.append("<span class=\"ER-op\"> =~ </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNRNode node) {
        sb.append("<span class=\"NR-op\"> !~ </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNotNode node) {
        sb.append("<span class=\"not-op\">!</span>");
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
            sb.append(String.format("<span class=\"query-property-marker\">%s</span>", fieldName));
        else
            sb.append(String.format("<span class=\"field\">%s</span>", fieldName));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNullLiteral node) {
        sb.append("<span class=\"null-value\">null</span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTTrueNode node) {
        sb.append("<span class=\"boolean-value\">true</span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTFalseNode node) {
        sb.append("<span class=\"boolean-value\">false</span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTStringLiteral node, String literal) {
        sb.append("<span class=\"string-value\">").append('\'').append(literal).append('\'').append("</span>");
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
    public void apply(StringBuilder sb, ASTMethodNode node, StringBuilder methodStringBuilder) {
        sb.append(String.format("<span class=\"method\">%s</span>", methodStringBuilder));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTNumberLiteral node) {
        sb.append(String.format("<span class=\"numeric-value\">%s</span>", node.image));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAdditiveOperator node) {
        sb.append(String.format("<span class=\"add-op\">%s</span>", node.image));
    }
    
    @Override
    public void apply(StringBuilder sb, ASTSizeMethod node) {
        sb.append("<span class=\"method\">.size() </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTMulNode node) {
        sb.append("<span class=\"mul-op\"> * </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTDivNode node) {
        sb.append("<span class=\"div-op\"> / </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTModNode node) {
        sb.append("<span class=\"mod-op\"> % </span>");
    }
    
    @Override
    public void apply(StringBuilder sb, ASTAssignment node, int i) {
        sb.append("<span class=\"assign-op\"> = </span>");
        if (i + 1 == node.jjtGetNumChildren())
            sb.setLength(sb.length() - "<span class=\"assign-op\"> = </span>".length());
    }
    
    @Override
    public void apply(StringBuilder sb, ASTUnaryMinusNode node) {
        sb.append("<span class=\"unary-minus\">-</span>");
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

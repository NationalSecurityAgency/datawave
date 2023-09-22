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

/**
 * For use in {@link JexlStringBuildingVisitor}. Used to decorate a Jexl Query with color.
 */
public interface JexlQueryDecorator {

    static final String NEWLINE = System.getProperty("line.separator");
    static final char SPACE = ' ';
    static final char SINGLE_QUOTE = '\'';

    public void apply(StringBuilder sb, ASTAdditiveOperator node);

    public void apply(StringBuilder sb, ASTAndNode node, Collection<String> childStrings, boolean needNewLines);

    public void apply(StringBuilder sb, ASTAssignment node, int i);

    public void apply(StringBuilder sb, ASTDivNode node);

    public void apply(StringBuilder sb, ASTEQNode node);

    public void apply(StringBuilder sb, ASTERNode node);

    public void apply(StringBuilder sb, ASTFalseNode node);

    public void apply(StringBuilder sb, ASTFunctionNode node, int i);

    public void apply(StringBuilder sb, ASTGENode node);

    public void apply(StringBuilder sb, ASTGTNode node);

    public void apply(StringBuilder sb, ASTIdentifier node);

    public void apply(StringBuilder sb, ASTLENode node);

    public void apply(StringBuilder sb, ASTLTNode node);

    public void apply(StringBuilder sb, ASTMethodNode node, StringBuilder methodStringBuilder);

    public void apply(StringBuilder sb, ASTModNode node);

    public void apply(StringBuilder sb, ASTMulNode node);

    public void apply(StringBuilder sb, ASTNENode node);

    public void apply(StringBuilder sb, ASTNotNode node);

    public void apply(StringBuilder sb, ASTNRNode node);

    public void apply(StringBuilder sb, ASTNullLiteral node);

    public void apply(StringBuilder sb, ASTNumberLiteral node);

    public void apply(StringBuilder sb, ASTOrNode node, Collection<String> childStrings, boolean needNewLines);

    public void apply(StringBuilder sb, ASTSizeMethod node);

    public void apply(StringBuilder sb, ASTStringLiteral node, String literal);

    public void apply(StringBuilder sb, ASTTrueNode node);

    public void apply(StringBuilder sb, ASTUnaryMinusNode node);

    /**
     * Removes the coloring given to a field element. Used to override coloring of the function namespace and function which are considered fields
     * (ASTIdentifiers) by default.
     *
     * @param sb
     */
    public void removeFieldColoring(StringBuilder sb);
}

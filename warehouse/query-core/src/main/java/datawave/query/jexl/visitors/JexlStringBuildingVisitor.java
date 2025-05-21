package datawave.query.jexl.visitors;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * A Jexl visitor which builds an equivalent Jexl query.
 *
 */
public class JexlStringBuildingVisitor extends BaseVisitor {
    protected static final Logger log = Logger.getLogger(JexlStringBuildingVisitor.class);
    protected static final char BACKSLASH = '\\';
    protected static final char STRING_QUOTE = '\'';

    // allowed methods for composition. Nothing that mutates the collection is allowed, thus we have:
    private Set<String> allowedMethods = Sets.newHashSet("contains", "retainAll", "containsAll", "isEmpty", "size", "equals", "hashCode", "getValueForGroup",
                    "getGroupsForValue", "getValuesForGroups", "toString", "values", "min", "max", "lessThan", "greaterThan", "compareWith");

    protected boolean sortDedupeChildren;

    public JexlStringBuildingVisitor() {
        this(false);
    }

    public JexlStringBuildingVisitor(boolean sortDedupeChildren) {
        this.sortDedupeChildren = sortDedupeChildren;
    }

    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return a string query
     */
    public static String buildQuery(JexlNode script, boolean sortDedupeChildren) {
        return buildQueryWithoutParse(script, sortDedupeChildren);
    }

    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return a query string
     */
    public static String buildQuery(JexlNode script) {
        return buildQuery(script, false);
    }

    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return a query string
     */
    public static String buildQueryWithoutParse(JexlNode script, boolean sortDedupeChildren) {
        JexlStringBuildingVisitor visitor = new JexlStringBuildingVisitor(sortDedupeChildren);

        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());

            s = sb.toString();
        } catch (StackOverflowError e) {

            throw e;
        }
        return s;
    }

    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return a query string
     */
    public static String buildQueryWithoutParse(JexlNode script) {
        return buildQueryWithoutParse(script, false);
    }

    public Object visit(ASTOrNode node, Object data) {

        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        JexlNode parent = node.jjtGetParent();
        boolean wrapIt = false;
        if (!(parent instanceof ASTReferenceExpression || parent instanceof ASTJexlScript || parent instanceof ASTOrNode || numChildren == 0)) {
            wrapIt = true;
            sb.append("(");
        }

        Collection<String> childStrings = (sortDedupeChildren) ? new TreeSet<>() : new ArrayList<>(numChildren);
        StringBuilder childSB = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, childSB);
            childStrings.add(childSB.toString());
            childSB.setLength(0);
        }
        sb.append(String.join(" || ", childStrings));

        if (wrapIt)
            sb.append(")");

        return data;
    }

    public Object visit(ASTAndNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        JexlNode parent = node.jjtGetParent();
        boolean wrapIt = false;
        if (!(parent instanceof ASTReferenceExpression || parent instanceof ASTJexlScript || parent instanceof ASTAndNode || numChildren == 0)) {
            wrapIt = true;
            sb.append("(");
        }

        Collection<String> childStrings = (sortDedupeChildren) ? new TreeSet<>() : new ArrayList<>(numChildren);
        StringBuilder childSB = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, childSB);
            childStrings.add(childSB.toString());
            childSB.setLength(0);
        }
        sb.append(String.join(" && ", childStrings));

        if (wrapIt)
            sb.append(")");

        return data;
    }

    private StringBuilder buildSimpleExpression(JexlNode node, String operand, StringBuilder sb) {
        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            QueryException qe = new QueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR,
                            "An " + node.getClass().getSimpleName() + " must have exactly two children");
            throw new IllegalArgumentException(qe);
        }

        node.jjtGetChild(0).jjtAccept(this, sb);

        sb.append(' ').append(operand).append(' ');

        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTEQNode node, Object data) {
        return buildSimpleExpression(node, "==", (StringBuilder) data);
    }

    public Object visit(ASTNENode node, Object data) {
        return buildSimpleExpression(node, "!=", (StringBuilder) data);
    }

    public Object visit(ASTLTNode node, Object data) {
        return buildSimpleExpression(node, "<", (StringBuilder) data);
    }

    public Object visit(ASTGTNode node, Object data) {
        return buildSimpleExpression(node, ">", (StringBuilder) data);
    }

    public Object visit(ASTLENode node, Object data) {
        return buildSimpleExpression(node, "<=", (StringBuilder) data);
    }

    public Object visit(ASTGENode node, Object data) {
        return buildSimpleExpression(node, ">=", (StringBuilder) data);
    }

    public Object visit(ASTERNode node, Object data) {
        return buildSimpleExpression(node, "=~", (StringBuilder) data);
    }

    public Object visit(ASTNRNode node, Object data) {
        return buildSimpleExpression(node, "!~", (StringBuilder) data);
    }

    public Object visit(ASTNotNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("!");
        node.childrenAccept(this, sb);

        return sb;
    }

    public Object visit(ASTIdentifier node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        String namespace = node.getNamespace();
        if (namespace != null) {
            sb.append(namespace);
            sb.append(":");
        }

        // We want to remove the $ if present and only replace it when necessary
        String fieldName = JexlASTHelper.rebuildIdentifier(JexlASTHelper.deconstructIdentifier(node.getName()));
        sb.append(fieldName);

        return sb;
    }

    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        sb.append(".");
        sb.append(node.getName());
        return sb;
    }

    public Object visit(ASTNullLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("null");
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTTrueNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("true");
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTFalseNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("false");
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTStringLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        String literal = node.getLiteral();

        JexlNode parent = node;
        do {
            parent = parent.jjtGetParent();
        } while (parent instanceof ASTReference);

        // escape any backslashes in the literal if this is not a regex node.
        // this is necessary to ensure that the query string created by this
        // visitor can be correctly parsed back into the current query tree.
        if (!(parent instanceof ASTERNode || parent instanceof ASTNRNode))
            literal = literal.replace(JexlASTHelper.SINGLE_BACKSLASH, JexlASTHelper.DOUBLE_BACKSLASH);

        int index = literal.indexOf(STRING_QUOTE);
        if (-1 != index) {
            // Slightly larger buffer
            int begin = 0;
            StringBuilder builder = new StringBuilder(literal.length() + 10);

            // Find every single quote and escape it
            while (-1 != index) {
                builder.append(literal.substring(begin, index));
                builder.append(BACKSLASH).append(STRING_QUOTE);
                begin = index + 1;
                index = literal.indexOf(STRING_QUOTE, begin);
            }

            // Tack on the end of the literal
            builder.append(literal.substring(begin));

            // Set the new version on the literal
            literal = builder.toString();
        }

        sb.append(STRING_QUOTE).append(literal).append(STRING_QUOTE);
        node.childrenAccept(this, sb);
        return sb;
    }

    @Override
    protected Object visit(ASTArguments node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        sb.append("(");

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            node.jjtGetChild(i).jjtAccept(this, sb);
        }

        sb.append(")");

        return sb;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        if (node.jjtGetNumChildren() > 0 && node.jjtGetChild(0) instanceof ASTIdentifierAccess) {
            ASTIdentifierAccess methodNameNode = (ASTIdentifierAccess) node.jjtGetChild(0);
            if (!allowedMethods.contains(methodNameNode.getName())) {
                QueryException qe = new QueryException(DatawaveErrorCode.METHOD_COMPOSITION_ERROR, MessageFormat.format("{0}", methodNameNode.getName()));
                throw new DatawaveFatalQueryException(qe);
            }
        }

        return node.childrenAccept(this, data);
    }

    public Object visit(ASTNumberLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(JexlNodes.getIdentifierOrLiteral(node));
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTReferenceExpression node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("(");
        int lastsize = sb.length();
        node.childrenAccept(this, sb);
        if (sb.length() == lastsize) {
            sb.setLength(sb.length() - 1);
        } else {
            sb.append(")");
        }
        return sb;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            sb.append("; ");
        }
        // cutting of the last ';' is still legal jexl and lets me not have to modify
        // all the unit tests that don't expect a trailing ';'
        if (sb.length() > 0) {
            sb.setLength(sb.length() - "; ".length());
        }
        return sb;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (i > 0) {
                sb.append(" + ");
            }
            node.jjtGetChild(i).jjtAccept(this, sb);
        }

        return sb;
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return buildSimpleExpression(node, "=^", (StringBuilder) data);
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return buildSimpleExpression(node, "!^", (StringBuilder) data);
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return buildSimpleExpression(node, "=$", (StringBuilder) data);
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return buildSimpleExpression(node, "!$", (StringBuilder) data);
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (i > 0) {
                sb.append(" - ");
            }
            node.jjtGetChild(i).jjtAccept(this, sb);
        }

        return sb;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" * ");
            }
        }

        return sb;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" / ");
            }
        }

        return sb;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" % ");
            }
        }

        return sb;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        boolean requiresParens = !(node.jjtGetParent() instanceof ASTReferenceExpression);
        StringBuilder sb = (StringBuilder) data;
        if (requiresParens)
            sb.append('(');
        for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            sb.append(" = ");
        }
        sb.setLength(sb.length() - " = ".length());
        if (requiresParens) {
            sb.append(')');
        }
        return sb;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("-");
        node.childrenAccept(this, sb);
        return sb;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("size");
        node.childrenAccept(this, sb);
        return sb;
    }
}

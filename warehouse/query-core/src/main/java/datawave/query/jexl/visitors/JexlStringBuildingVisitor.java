package datawave.query.jexl.visitors;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
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
import org.apache.commons.jexl2.parser.ASTJexlScript;
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
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.collections.FunctionalSet;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.TreeSet;

/**
 * A Jexl visitor which builds an equivalent Jexl query. Can be built in a formatted (with {@link #buildDecoratedQuery}) or unformatted manner (with
 * {@link #buildQuery} or {@link #buildQueryWithoutParse}). Formatting includes expanding the query to multiple indented lines and optional coloring of the
 * different query elements.
 */
public class JexlStringBuildingVisitor extends BaseVisitor {
    protected static final Logger log = Logger.getLogger(JexlStringBuildingVisitor.class);
    protected static final String NEWLINE = System.getProperty("line.separator");
    protected static final char DOUBLE_QUOTE = '\"';
    protected static final char BACKSLASH = '\\';
    protected static final char STRING_QUOTE = '\'';

    // allowed methods for composition. Nothing that mutates the collection is allowed, thus we have:
    private Set<String> allowedMethods = Sets.newHashSet("contains", "retainAll", "containsAll", "isEmpty", "size", "equals", "hashCode", "getValueForGroup",
                    "getGroupsForValue", "getValuesForGroups", "toString", "values", "min", "max", "lessThan", "greaterThan", "compareWith");

    protected boolean sortDedupeChildren;
    // Whether or not the query should be expanded to multiple lines
    protected boolean buildMultipleLines;
    protected JexlQueryDecorator decorator;
    
    /**
     * Default constructor. Visitor that will apply no formatting.
     */
    public JexlStringBuildingVisitor() {
        this(false);
    }
    
    /**
     * Visitor that will apply no formatting.
     * 
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     */
    public JexlStringBuildingVisitor(boolean sortDedupeChildren) {
        this(sortDedupeChildren, false);
    }
    
    /**
     * Visitor that will apply no color decoration but may expand the query to multiple indented lines.
     * 
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @param buildMultipleLines
     *            Whether or not to format the query on multiple indented lines
     */
    public JexlStringBuildingVisitor(boolean sortDedupeChildren, boolean buildMultipleLines) {
        this(sortDedupeChildren, buildMultipleLines, new EmptyDecorator());
    }
    
    /**
     * Visitor that may apply color decoration and may expand the query to multiple indented lines
     * 
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @param buildMultipleLines
     *            Whether or not to format the query on multiple indented lines
     * @param decorator
     *            The type of decoration which will be used
     */
    public JexlStringBuildingVisitor(boolean sortDedupeChildren, boolean buildMultipleLines, JexlQueryDecorator decorator) {
        this.sortDedupeChildren = sortDedupeChildren;
        this.buildMultipleLines = buildMultipleLines;
        this.decorator = decorator;
    }
    
    /**
     * Given two query strings (a decorated version and a plain version) separated into lines consisting of expressions, open parenthesis, and closing
     * parenthesis, format the decorated string to be indented properly (add necessary number of tab characters (4 spaces) to each line). This is called after
     * all the nodes have been visited to finalize the formatting of the query.
     * 
     * @param decoratedQueryStr
     * @param plainQueryStr
     * @return the final formatted version of the decoratedQueryStr.
     */
    private static String formatBuiltQuery(String decoratedQueryStr, String plainQueryStr) {
        String res = "";
        int numTabs = 0;
        
        String[] decoratedLines = decoratedQueryStr.split(NEWLINE);
        String[] plainLines = plainQueryStr.split(NEWLINE);
        String decoratedLine = null;
        String plainLine = null;
        // Go through all the lines of the undecorated query string, but update the decoratedQueryString.
        // This is done to keep methods like containsOnly() as simple as possible.
        for (int i = 0; i < plainLines.length; i++) {
            decoratedLine = decoratedLines[i];
            plainLine = plainLines[i];
            
            if (containsOnly(plainLine, '(')) {
                // Add tabs to result then increase the number of tabs
                for (int j = 0; j < numTabs; j++) {
                    res += "    ";
                }
                numTabs++;
            } else if (containsOnly(plainLine, ')') || closeParensFollowedByAndOr(plainLine)) {
                // Decrease number of tabs then add tabs to result
                numTabs--;
                for (int j = 0; j < numTabs; j++) {
                    res += "    ";
                }
            } else {
                // Add tabs to result
                for (int j = 0; j < numTabs; j++) {
                    res += "    ";
                }
            }
            if (i != plainLines.length - 1) {
                res += decoratedLine + NEWLINE;
            } else {
                res += decoratedLine;
            }
        }
        
        return res;
    }
    
    /**
     * Returns true if str contains only ch characters (1 or more). False otherwise.
     * 
     * @param str
     * @param ch
     * @return
     */
    private static boolean containsOnly(String str, char ch) {
        return str.matches("^[" + ch + "]+$");
    }
    
    /**
     * Returns true if a string contains only closing parenthesis (1 or more) followed by the and or or operator
     * 
     * @param str
     * @return
     */
    private static boolean closeParensFollowedByAndOr(String str) {
        return str.matches("^([)]+ (&&|\\|\\|) )$");
    }
    
    /**
     * Determines whether a JexlNode should be formatted on multiple lines or not. The JexlNodes which may be split into multiple lines are ASTOrNodes,
     * ASTReferenceExpressions, and ASTAndNodes. In all cases: If buildMultipleLines is false, false is immediately returned. Or if the given node has an
     * ASTAndNode ancestor which returns false for needNewLines(), immediately return false for the given node (anything that ancestor node is 'anded' with
     * should be on the same line).
     * 
     * Further logic for splitting ASTOrNodes: True otherwise
     * 
     * Further logic for splitting ASTReferenceExpressions: If the node has a child ASTAndNode or ASTOrNode and that child needsNewLines(), return true. False
     * otherwise
     * 
     * Further logic for splitting ASTAndNodes: If this node is a bounded marker node OR if this node is a marker node which has a child bounded marker node OR
     * if this node is a marker node with a single term as a child, then we don't want to add any new lines on this visit or on visits to this nodes children
     * (return false). Otherwise, return true.
     * 
     * @param node
     * @return
     */
    private boolean needNewLines(JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        boolean needNewLines;
        
        if (buildMultipleLines == false) { // Always return false if the query is meant to be built without multiple lines in the first place
            return false;
        }
        
        // We want to go up the tree from this node to see if any parent ASTAndNodes return false for needNewLines().
        // If this is the case, we also want to return false for this current node. (whenever needNewLines() returns false
        // for an ASTAndNode, the children should also be printed on the same line)
        while (parent != null) {
            if (parent instanceof ASTAndNode && !needNewLines(parent)) {
                return false;
            }
            parent = parent.jjtGetParent();
        }
        
        // Or nodes are always split to multiple lines, unless buildMultipleLines is false or if a parent ASTAndNode returns false for needNewLines
        if (node instanceof ASTOrNode) {
            return true;
        } else if (node instanceof ASTReferenceExpression) {
            JexlNode child = node.jjtGetChild(0);
            needNewLines = false;
            
            if ((child instanceof ASTAndNode || child instanceof ASTOrNode) && needNewLines(child)) {
                needNewLines = true;
            }
            return needNewLines;
        } else if (node instanceof ASTAndNode) {
            int numChildren = node.jjtGetNumChildren();
            needNewLines = true;
            // Whether or not this node has a child with a bounded range query
            boolean childHasBoundedRange = false;
            // Whether or not this node is a marker node which has a child bounded marker node
            boolean markerWithSingleTerm = false;
            
            for (int i = 0; i < numChildren; i++) {
                if (QueryPropertyMarker.findInstance(node.jjtGetChild(i)).isType(BoundedRange.class)) {
                    childHasBoundedRange = true;
                }
            }
            
            if (numChildren == 2) {
                if (QueryPropertyMarker.findInstance(node).isAnyType() && node.jjtGetChild(1) instanceof ASTReference
                                && node.jjtGetChild(1).jjtGetChild(0) instanceof ASTReferenceExpression
                                && !(node.jjtGetChild(1).jjtGetChild(0).jjtGetChild(0) instanceof ASTAndNode)
                                && !(node.jjtGetChild(1).jjtGetChild(0).jjtGetChild(0) instanceof ASTOrNode)) {
                    markerWithSingleTerm = true;
                }
            }
            
            // If this node is a bounded marker node OR if this node is a marker node which has a child bounded marker node
            // OR if this node is a marker node with a single term as a child, then
            // we don't want to add any new lines on this visit or on visits to this nodes children
            if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class)
                            || (QueryPropertyMarker.findInstance(node).isAnyType() && childHasBoundedRange) || markerWithSingleTerm) {
                needNewLines = false;
            }
            
            return needNewLines;
        } else {
            return false;
        }
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

        JexlStringBuildingVisitor visitor = new JexlStringBuildingVisitor(sortDedupeChildren);

        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());

            s = sb.toString();

            try {
                JexlASTHelper.parseJexlQuery(s);
            } catch (ParseException e) {
                log.error("Could not parse JEXL AST after performing transformations to run the query", e);

                for (String line : PrintingVisitor.formattedQueryStringList(script)) {
                    log.error(line);
                }
                log.error("");

                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_EXECUTION_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
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
    
    /**
     * Build a String that is the equivalent JEXL query with decoration.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @param buildMultipleLines
     *            Whether or not the query should be formatted on multiple lines
     * @param decorator
     *            How the query will be decorated (e.g., decoration for Bash, HTML, or no added decoration (EmptyDecorator))
     * @return
     */
    public static String buildDecoratedQuery(JexlNode script, boolean sortDedupeChildren, boolean buildMultipleLines, JexlQueryDecorator decorator) {
        // Two visitors to keep containsOnly() and closeParensFollowedByAndOr() as simple as possible
        JexlStringBuildingVisitor decoratedVisitor = new JexlStringBuildingVisitor(sortDedupeChildren, buildMultipleLines, decorator);
        JexlStringBuildingVisitor plainVisitor = new JexlStringBuildingVisitor(sortDedupeChildren, buildMultipleLines, new EmptyDecorator());
        
        String decoratedQueryStr = null, plainQueryStr = null;
        try {
            StringBuilder decoratedStringBuilder = (StringBuilder) script.jjtAccept(decoratedVisitor, new StringBuilder());
            StringBuilder plainStringBuilder = (StringBuilder) script.jjtAccept(plainVisitor, new StringBuilder());
            
            decoratedQueryStr = decoratedStringBuilder.toString();
            plainQueryStr = plainStringBuilder.toString();
        } catch (StackOverflowError e) {
            
            throw e;
        }
        return formatBuiltQuery(decoratedQueryStr, plainQueryStr);
    }
    
    public Object visit(ASTOrNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        JexlNode parent = node.jjtGetParent();
        boolean wrapIt = false;
        boolean needNewLines = needNewLines(node);
        
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
        
        decorator.apply(sb, node, childStrings, needNewLines);
        
        if (wrapIt)
            sb.append(")");

        return data;
    }

    public Object visit(ASTAndNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        boolean needNewLines = needNewLines(node);
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
        
        decorator.apply(sb, node, childStrings, needNewLines);
        
        if (wrapIt)
            sb.append(")");

        return data;
    }

    public Object visit(ASTEQNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTEQNode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTNENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTNENode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTLTNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTLTNode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTGTNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTGTNode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTLENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTLENode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTGENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTGENode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTERNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTERNode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTNRNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        int numChildren = node.jjtGetNumChildren();

        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTERNode has more than two children");
        }

        node.jjtGetChild(0).jjtAccept(this, sb);
        
        decorator.apply(sb, node);
        
        node.jjtGetChild(1).jjtAccept(this, sb);

        return sb;
    }

    public Object visit(ASTNotNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);

        return sb;
    }

    public Object visit(ASTIdentifier node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);

        return sb;
    }

    public Object visit(ASTNullLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTTrueNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTFalseNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTStringLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        String literal = node.image;

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
        
        decorator.apply(sb, node, literal);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTFunctionNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (1 == i) {
                sb.append(":");
            } else if (2 == i) {
                sb.append("(");
            } else if (2 < i) {
                sb.append(", ");
            }
            
            decorator.apply(sb, node, i);
            
            node.jjtGetChild(i).jjtAccept(this, sb);
            
            if (i == 0 || i == 1) {
                // Remove the field coloring given to the function namespace (i = 0) and the function (i = 1)
                decorator.removeFieldColoring(sb);
            }
        }

        sb.append(")");

        return sb;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        StringBuilder methodStringBuilder = new StringBuilder();
        StringBuilder argumentStringBuilder = new StringBuilder();
        int kidCount = node.jjtGetNumChildren();
        for (int i = 0; i < kidCount; i++) {
            if (i == 0) {
                JexlNode methodNode = node.jjtGetChild(i);
                methodStringBuilder.append(".");
                if (FunctionalSet.allowedMethods.contains(methodNode.image) == false) {
                    QueryException qe = new QueryException(DatawaveErrorCode.METHOD_COMPOSITION_ERROR, MessageFormat.format("{0}", methodNode.image));
                    throw new DatawaveFatalQueryException(qe);
                }
                methodStringBuilder.append(methodNode.image);
                methodStringBuilder.append("("); // parens are open. don't forget to close
            } else {
                // adding any method arguments
                JexlNode argumentNode = node.jjtGetChild(i);
                if (argumentNode instanceof ASTReference) {
                    // a method may have an argument that is another method. In this case, descend the visit tree for it
                    if (JexlASTHelper.HasMethodVisitor.hasMethod(argumentNode)) {
                        this.visit((ASTReference) argumentNode, argumentStringBuilder);
                    } else {
                        for (int j = 0; j < argumentNode.jjtGetNumChildren(); j++) {
                            JexlNode argKid = argumentNode.jjtGetChild(j);
                            if (argKid instanceof ASTFunctionNode) {
                                this.visit((ASTFunctionNode) argKid, argumentStringBuilder);
                            } else {
                                if (argumentStringBuilder.length() > 0) {
                                    argumentStringBuilder.append(",");
                                }
                                if (argKid instanceof ASTStringLiteral) {
                                    argumentStringBuilder.append("'");
                                }
                                argumentStringBuilder.append(argKid.image);
                                if (argKid instanceof ASTStringLiteral) {
                                    argumentStringBuilder.append("'");
                                }
                            }
                        }
                    }
                } else if (argumentNode instanceof ASTNumberLiteral) {
                    if (argumentStringBuilder.length() > 0) {
                        argumentStringBuilder.append(",");
                    }
                    argumentStringBuilder.append(argumentNode.image);
                }
            }
        }
        methodStringBuilder.append(argumentStringBuilder);
        methodStringBuilder.append(")"); // close parens in method
        
        decorator.apply(sb, node, methodStringBuilder);
        
        return sb;
    }

    public Object visit(ASTNumberLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    public Object visit(ASTReferenceExpression node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        boolean needNewLines = needNewLines(node);
        
        sb.append("(" + (needNewLines ? NEWLINE : ""));
        
        int lastsize = sb.length();
        node.childrenAccept(this, sb);
        if (sb.length() == lastsize) {
            sb.setLength(sb.length() - 1);
        } else {
            sb.append((needNewLines ? NEWLINE : "") + ")");
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
    public Object visit(ASTAdditiveOperator node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
        }

        return sb;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                decorator.apply(sb, node);
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
                decorator.apply(sb, node);
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
                decorator.apply(sb, node);
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
            decorator.apply(sb, node, i);
        }
        if (requiresParens) {
            sb.append(')');
        }
        return sb;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }
    
    private static String getHelp() {
        return "Usage:" + NEWLINE + "JexlStringBuildingVisitor [ --help | --BashMultipleLines | --BashSingleLine | --HtmlMultipleLines | "
                        + "--HtmlSingleLine | --NoDecSingleLine | --NoDecMultipleLines ] {JEXL-QUERY-FILE(s)}" + NEWLINE + "or" + NEWLINE
                        + "JexlStringBuildingVisitor [ --help | --BashMultipleLines | --BashSingleLine | --HtmlMultipleLines | "
                        + "--HtmlSingleLine | --NoDecSingleLine | --NoDecMultipleLines ]" + NEWLINE + NEWLINE + "Description:" + NEWLINE
                        + "The JexlStringBuildingVisitor program parses one or more JEXL query files, specified on the command" + NEWLINE
                        + "line or the standard input if no filenames are provided. " + NEWLINE + "It prints various types of "
                        + "output, depending upon the options selected." + NEWLINE + "It is useful for detecting errors in the JEXL query "
                        + "and providing a formatted output." + NEWLINE + NEWLINE + "Options:" + NEWLINE + "--help" + NEWLINE
                        + "    Displays how to use this program" + NEWLINE + "--BashMultipleLines" + NEWLINE
                        + "    Formats each of the queries on multiple lines with bash color decoration" + NEWLINE + "--BashSingleLine" + NEWLINE
                        + "    Formats each of the queries on a single line with bash color decoration" + NEWLINE + "--HtmlMultipleLines" + NEWLINE
                        + "    Formats each of the queries on multiple lines with HTML color decoration" + NEWLINE + "--HtmlSingleLine" + NEWLINE
                        + "    Formats each of the queries on a single line with HTML color decoration" + NEWLINE + "--NoDecMultipleLines" + NEWLINE
                        + "    Formats each of the queries on multiple lines with no color decoration" + NEWLINE + "--NoDecSingleLine" + NEWLINE
                        + "    Formats each of the queries on a single line with no color decoration" + NEWLINE;
    }
    
    public static void main(String args[]) {
        JexlQueryDecorator decorator = null;
        BufferedReader fileReader, cmdReader;
        String query, builtQuery;
        JexlNode node;
        boolean buildMultipleLines = false;
        
        if (args.length == 0) { // No args provided: print how this class is meant to be used
            System.out.println("ERROR: no args provided" + NEWLINE + getHelp());
        } else if (args.length == 1) { // One arg: should be one of the valid options (e.g., --BashMultipleLines, --BashSingleLine, etc.)
            if (args[0].equals("--help")) {
                System.out.println(getHelp());
            } else {
                ArrayList<String> argsList = new ArrayList<String>();
                argsList.add(args[0]);
                // Prompt user to enter the file(s) to parse
                System.out.println("Enter file names followed by 'done'");
                Scanner sc = new Scanner(System.in);
                String line = "";
                do {
                    line = sc.next();
                    if (!line.equals("done"))
                        argsList.add(line);
                } while (!line.equals("done"));
                sc.close();
                
                args = new String[argsList.size()];
                args = argsList.toArray(args);
            }
        }
        
        if (args.length >= 2) {
            if (args[0].equals("--BashMultipleLines")) {
                decorator = new BashDecorator();
                buildMultipleLines = true;
            } else if (args[0].equals("--BashSingleLine")) {
                decorator = new BashDecorator();
                buildMultipleLines = false;
            } else if (args[0].equals("--HtmlMultipleLines")) {
                decorator = new HtmlDecorator();
                buildMultipleLines = true;
            } else if (args[0].equals("--HtmlSingleLine")) {
                decorator = new HtmlDecorator();
                buildMultipleLines = false;
            } else if (args[0].equals("--NoDecMultipleLines")) {
                decorator = new EmptyDecorator();
                buildMultipleLines = true;
            } else if (args[0].equals("--NoDecSingleLine")) {
                decorator = new EmptyDecorator();
                buildMultipleLines = false;
            } else if (args[0].equals("--help")) {
                System.out.println(getHelp());
            } else {
                System.out.println("ERROR: Invalid option provided." + NEWLINE + getHelp());
            }
        }
        
        if (decorator != null) {
            for (int i = 1; i < args.length; i++) {
                try {
                    String path = args[i].replaceFirst("^~", System.getProperty("user.home"));
                    fileReader = new BufferedReader(new FileReader(path));
                    String fileReaderLine = fileReader.readLine();
                    while (fileReaderLine != null) {
                        query = fileReaderLine;
                        
                        node = JexlASTHelper.parseJexlQuery(query);
                        builtQuery = buildDecoratedQuery(node, false, buildMultipleLines, decorator);
                        String echoCommand = "echo " + DOUBLE_QUOTE + builtQuery + DOUBLE_QUOTE;
                        String[] commands = {"sh", "-c", echoCommand};
                        Process process = Runtime.getRuntime().exec(commands);
                        cmdReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String cmdReaderLine = cmdReader.readLine();
                        while (cmdReaderLine != null) {
                            System.out.println(cmdReaderLine);
                            cmdReaderLine = cmdReader.readLine();
                        }
                        
                        fileReaderLine = fileReader.readLine();
                    }
                } catch (FileNotFoundException e) {
                    System.out.println("ERROR: Failed to find given file." + NEWLINE + getHelp());
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    System.out.println("ERROR: Failed to parse query." + NEWLINE + getHelp());
                }
            }
        }
    }
}

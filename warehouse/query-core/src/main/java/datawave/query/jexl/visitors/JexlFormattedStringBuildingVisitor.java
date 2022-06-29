package datawave.query.jexl.visitors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
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

import datawave.microservice.querymetric.QueryMetric;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import com.google.common.collect.Sets;

/**
 * A Jexl visitor which builds an equivalent Jexl query in a formatted manner. Formatted meaning parenthesis are added on one line, and children are added on a
 * new, indented line. Additionally, there is the option to decorate the query with added color for easier readability in terminal or on web pages. Same
 * functionality as JexlStringBuildingVisitor.java except the query is built in a formatted manner.
 */
public class JexlFormattedStringBuildingVisitor extends JexlStringBuildingVisitor {
    protected static final String NEWLINE = System.getProperty("line.separator");
    protected static final String DOUBLE_QUOTE = "\"";
    
    // allowed methods for composition. Nothing that mutates the collection is allowed, thus we have:
    private Set<String> allowedMethods = Sets.newHashSet("contains", "retainAll", "containsAll", "isEmpty", "size", "equals", "hashCode", "getValueForGroup",
                    "getGroupsForValue", "getValuesForGroups", "toString", "values", "min", "max", "lessThan", "greaterThan", "compareWith");
    
    private JexlQueryDecorator decorator;
    
    public JexlFormattedStringBuildingVisitor() {
        this(false);
    }

    public JexlFormattedStringBuildingVisitor(boolean sortDedupeChildren) {
        this.sortDedupeChildren = sortDedupeChildren;
        this.decorator = new EmptyDecorator();
    }
    
    public JexlFormattedStringBuildingVisitor(boolean sortDedupeChildren, JexlQueryDecorator decorator) {
        this.sortDedupeChildren = sortDedupeChildren;
        this.decorator = decorator;
    }

    /**
     * Given a query String separated into lines consisting of expressions, open parenthesis, and closing parenthesis, format the string to be indented properly
     * (add necessary number of tab characters to each line). This is called after all the nodes have been visited to finalize the formatting of the query.
     *
     * @param query
     *            a query
     * @return the final formatted String
     */
    private static String formatBuiltQuery(String query) {
        String res = "";
        int numTabs = 0;

        String[] lines = query.split(NEWLINE);
        // Go through all the lines
        for (String line : lines) {
            if (containsOnly(line, '(')) {
                // Add tabs to result then increase the number of tabs
                for (int i = 0; i < numTabs; i++) {
                    res += "    ";
                }
                numTabs++;
            } else if (containsOnly(line, ')') || closeParensFollowedByAndOr(line)) {
                // Decrease number of tabs then add tabs to result
                numTabs--;
                for (int i = 0; i < numTabs; i++) {
                    res += "    ";
                }
            } else {
                // Add tabs to result
                for (int i = 0; i < numTabs; i++) {
                    res += "    ";
                }
            }
            if (line != lines[lines.length - 1]) {
                res += line + NEWLINE;
            } else {
                res += line;
            }
        }

        return res;
    }

    /**
     * Returns true if str contains only ch characters (1 or more). False otherwise.
     *
     * @param str
     *            the str
     * @param ch
     *            the char
     * @return boolean
     */
    private static boolean containsOnly(String str, char ch) {
        str = str.replaceAll("<[^>]*>", ""); // remove all existing html from the string (if present)
        str = removeAllBashColoring(str);
        
        return str.matches("^[" + ch + "]+$");
    }

    /**
     * Returns true if a string contains only closing parenthesis (1 or more) followed by the and or or operator
     *
     * @param str
     *            the str
     * @return boolean
     */
    private static boolean closeParensFollowedByAndOr(String str) {
        str = str.replaceAll("<[^>]*>", ""); // remove all existing html from the string (if present)
        str = removeAllBashColoring(str);
        
        return str.matches("^([)]+ (&&|\\|\\|) )$");
    }

    /**
     * Removes all bash coloring from the given string and returns it
     * 
     * @param str
     * @return
     */
    private static String removeAllBashColoring(String str) {
        int indexStartOfColor = str.indexOf("\\e");
        while (indexStartOfColor != -1) {
            int indexEndOfColor = str.indexOf("m", indexStartOfColor);
            str = str.replace(str.substring(indexStartOfColor, indexEndOfColor + 1), ""); // remove the existing bash color formatting from the string (if
                                                                                          // present)
            indexStartOfColor = str.indexOf("\\e");
        }
        return str;
    }
    
    /**
     * Determines whether a JexlNode should be formatted on multiple lines or not. If this node is a bounded marker node OR if this node is a marker node which
     * has a child bounded marker node OR if this node is a marker node with a single term as a child, then return false (should all be one line). Otherwise,
     * return true.
     *
     * @param node
     *            a node
     * @return boolean
     */
    private static boolean needNewLines(JexlNode node) {
        int numChildren = node.jjtGetNumChildren();
        boolean needNewLines = true;
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
        if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class) || (QueryPropertyMarker.findInstance(node).isAnyType() && childHasBoundedRange)
                        || markerWithSingleTerm) {
            needNewLines = false;
        }

        return needNewLines;
    }

    /**
     * Build a String that is the equivalent JEXL query formatted on multiple lines. No added Bash or HTML coloring.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return the query string
     */
    public static String buildQuery(JexlNode script, boolean sortDedupeChildren) {
        
        JexlFormattedStringBuildingVisitor visitor = new JexlFormattedStringBuildingVisitor(sortDedupeChildren);

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
        return formatBuiltQuery(s);
    }

    /**
     * Build a String that is the equivalent JEXL query formatted on multiple lines. No added Bash or HTML coloring.
     *
     * @param script
     *            An ASTJexlScript
     * @return query string
     */
    public static String buildQuery(JexlNode script) {
        return buildQuery(script, false);
    }

    /**
     * Build a String that is the equivalent JEXL query with HTML color styling and formatted on multiple lines.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return query string
     */
    public static String buildQueryWithHtmlFormatting(JexlNode script, boolean sortDedupeChildren) {
        JexlFormattedStringBuildingVisitor visitor = new JexlFormattedStringBuildingVisitor(sortDedupeChildren, new HtmlDecorator());
        
        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());

            s = sb.toString();
        } catch (StackOverflowError e) {

            throw e;
        }
        return formatBuiltQuery(s);
    }

    /**
     * Build a String that is the equivalent JEXL query with HTML color styling and formatted on multiple lines.
     *
     * @param script
     *            An ASTJexlScript
     * @return query string
     */
    public static String buildQueryWithHtmlFormatting(JexlNode script) {
        return buildQueryWithHtmlFormatting(script, false);
    }
    
    /**
     * Build a String that is the equivalent JEXL query with Bash color styling and formatted on multiple lines.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return
     */
    public static String buildQueryWithBashFormatting(JexlNode script, boolean sortDedupeChildren) {
        JexlFormattedStringBuildingVisitor visitor = new JexlFormattedStringBuildingVisitor(sortDedupeChildren, new BashDecorator());
        
        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());
            
            s = sb.toString();
        } catch (StackOverflowError e) {
            
            throw e;
        }
        return formatBuiltQuery(s);
    }
    
    /**
     * Build a String that is the equivalent JEXL query with Bash color styling and formatted on multiple lines.
     *
     * @param script
     *            An ASTJexlScript
     * @return
     */
    public static String buildQueryWithBashFormatting(JexlNode script) {
        return buildQueryWithBashFormatting(script, false);
    }
    
    @Override
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
        
        decorator.apply(sb, node, childStrings);
        
        if (wrapIt)
            sb.append(")");

        return data;
    }

    @Override
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
        Collection<String> childStringsFormatted = (sortDedupeChildren) ? new TreeSet<>() : new ArrayList<>(numChildren);
        StringBuilder childSB = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, childSB);
            childStrings.add(childSB.toString());
            childSB.setLength(0);
        }
        // If needNewLines is false, we should remove the new lines added to the child strings
        for (String childString : childStrings) {
            childStringsFormatted.add(needNewLines ? childString : childString.replace(NEWLINE, ""));
        }
        
        decorator.apply(sb, node, childStringsFormatted, needNewLines);
        
        if (wrapIt)
            sb.append(")");

        return data;
    }

    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
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
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        
        return sb;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        
        return sb;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
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
    
    @Override
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
                if (decorator instanceof HtmlDecorator) {
                    // Remove the child <span> element if present (don't remove </span>)
                    String strToRem = "<span class=\"field\">";
                    
                    int indexOfStr = sb.indexOf(strToRem);
                    if (indexOfStr != -1) {
                        sb.delete(indexOfStr, indexOfStr + strToRem.length());
                    }
                } else if (decorator instanceof BashDecorator) {
                    // Remove the field coloring given to the function namespace (if i = 0) and remove the field coloring given to the function (if i = 1)
                    int indexOfStr = sb.indexOf(BashDecorator.FIELD_COLOR);
                    if (indexOfStr != -1) {
                        sb.delete(indexOfStr, sb.indexOf("m", indexOfStr) + 1);
                    }
                }
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
                if (allowedMethods.contains(methodNode.image) == false) {
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
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        decorator.apply(sb, node);
        
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode child = node.jjtGetChild(0);
        boolean needNewLines = false;

        if ((child instanceof ASTAndNode || child instanceof ASTOrNode) && needNewLines(child)) {
            needNewLines = true;
        }
        StringBuilder sb = (StringBuilder) data;
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
    
    /**
     * Returns a new list of QueryMetrics which is identical to the given list except the query and query plan for each metric are formatted according to
     * {@link #buildQueryWithHtmlFormatting(JexlNode) buildQueryWithHtmlFormatting}
     * 
     * @param metrics
     * @return
     */
    public static List<QueryMetric> formatMetrics(List<QueryMetric> metrics) {
        List<QueryMetric> updatedMetrics = new ArrayList<QueryMetric>();

        // For each metric, update the query to be formatted (if applicable) and update
        // the plan to be formatted
        for (QueryMetric metric : metrics) {
            JexlNode queryNode = null, planNode = null;
            QueryMetric updatedMetric = new QueryMetric(metric);
            String query = updatedMetric.getQuery();
            String plan = updatedMetric.getPlan();
            // If it is a JEXL query, set the query to be formatted
            if (query != null && isJexlQuery(metric.getParameters())) {
                try {
                    queryNode = JexlASTHelper.parseJexlQuery(query);
                    updatedMetric.setQuery(buildQueryWithHtmlFormatting(queryNode));
                } catch (ParseException e) {
                    log.error("Could not parse JEXL AST after performing transformations to run the query", e);

                    if (log.isTraceEnabled()) {
                        log.trace(PrintingVisitor.formattedQueryString(queryNode));
                    }
                }
            }
            // Format the plan (plan will always be a JEXL query)
            if (plan != null) {
                try {
                    planNode = JexlASTHelper.parseJexlQuery(plan);
                    updatedMetric.setPlan(buildQueryWithHtmlFormatting(planNode));
                } catch (ParseException e) {
                    log.error("Could not parse JEXL AST after performing transformations to run the query", e);

                    if (log.isTraceEnabled()) {
                        log.trace(PrintingVisitor.formattedQueryString(planNode));
                    }
                }
            }
            updatedMetrics.add(updatedMetric);
        }

        return updatedMetrics;
    }

    private static boolean isJexlQuery(Set<Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }

    public static void main(String args[]) {
        String query;

        if (args.length != 1) {
            Scanner scanner = new Scanner(System.in);
            query = scanner.nextLine();
            scanner.close();
        } else {
            query = args[0];
        }
        try {
            String echoCommand = "echo " + DOUBLE_QUOTE + buildQueryWithBashFormatting(JexlASTHelper.parseJexlQuery(query)) + DOUBLE_QUOTE;
            String[] commands = {"sh", "-c", echoCommand};
            Process process = Runtime.getRuntime().exec(commands);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (ParseException e) {
            System.out.println("Failure to parse given query.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * A Jexl visitor which builds an equivalent Jexl query in a formatted manner. Formatted meaning parenthesis are added on one line, and children are added on a
 * new, indented line. Same functionality as JexlStringBuildingVisitor.java except the query is built in a formatted manner.
 */
public class JexlFormattedStringBuildingVisitor extends JexlStringBuildingVisitor {
    protected static final String NEWLINE = System.getProperty("line.separator");

    public JexlFormattedStringBuildingVisitor() {
        super(false);
    }

    public JexlFormattedStringBuildingVisitor(boolean sortDedupeChildren) {
        super(sortDedupeChildren);
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
        return str.matches("^([)]+ (&&|\\|\\|) )$");
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
            if (QueryPropertyMarker.findInstance(node.jjtGetChild(i)).isType(BOUNDED_RANGE)) {
                childHasBoundedRange = true;
            }
        }

        if (numChildren == 2) {
            if (QueryPropertyMarker.findInstance(node).isAnyType() && node.jjtGetChild(1) instanceof ASTReferenceExpression
                            && !(node.jjtGetChild(1).jjtGetChild(0) instanceof ASTAndNode) && !(node.jjtGetChild(1).jjtGetChild(0) instanceof ASTOrNode)) {
                markerWithSingleTerm = true;
            }
        }

        // If this node is a bounded marker node OR if this node is a marker node which has a child bounded marker node
        // OR if this node is a marker node with a single term as a child, then
        // we don't want to add any new lines on this visit or on visits to this nodes children
        if (QueryPropertyMarker.findInstance(node).isType(BOUNDED_RANGE) || (QueryPropertyMarker.findInstance(node).isAnyType() && childHasBoundedRange)
                        || markerWithSingleTerm) {
            needNewLines = false;
        }

        return needNewLines;
    }

    /**
     * Build a String that is the equivalent JEXL query.
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
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return query string
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
     * @return query string
     */
    public static String buildQueryWithoutParse(JexlNode script, boolean sortDedupeChildren) {
        JexlFormattedStringBuildingVisitor visitor = new JexlFormattedStringBuildingVisitor(sortDedupeChildren);

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
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return query string
     */
    public static String buildQueryWithoutParse(JexlNode script) {
        return buildQueryWithoutParse(script, false);
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
        sb.append(String.join(" || " + NEWLINE, childStrings));

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
        sb.append(String.join(" && " + (needNewLines ? NEWLINE : ""), childStringsFormatted));

        if (wrapIt)
            sb.append(")");

        return data;
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

    public static <T extends BaseQueryMetric> List<T> formatMetrics(List<T> metrics) {
        List<T> updatedMetrics = new ArrayList<>();
        // For each metric, update the query to be formatted (if applicable) and update
        // the plan to be formatted
        for (BaseQueryMetric metric : metrics) {
            JexlNode queryNode = null, planNode = null;
            T updatedMetric = (T) metric.duplicate();
            String query = updatedMetric.getQuery();
            String plan = updatedMetric.getPlan();
            // If it is a JEXL query, set the query to be formatted
            if (query != null && isJexlQuery(metric.getParameters())) {
                try {
                    queryNode = JexlASTHelper.parseJexlQuery(query);
                    updatedMetric.setQuery(buildQuery(queryNode));
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
                    updatedMetric.setPlan(buildQuery(planNode));
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
            System.out.println(JexlFormattedStringBuildingVisitor.buildQuery(JexlASTHelper.parseJexlQuery(query)));
        } catch (ParseException e) {
            System.out.println("Failure to parse given query.");
        }
    }
}

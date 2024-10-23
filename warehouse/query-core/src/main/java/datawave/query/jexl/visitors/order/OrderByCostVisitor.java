package datawave.query.jexl.visitors.order;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.DefaultJexlNodeComparator;
import datawave.query.jexl.nodes.FieldCostComparator;
import datawave.query.jexl.nodes.JexlNodeComparator;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.TermCostComparator;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Orders query nodes by cost using one or more {@link JexlNodeComparator}s.
 * <p>
 * The {@link DefaultJexlNodeComparator} orders a query based on the implied cost via the node id, see {@link ParserTreeConstants}. In general an EQ node is
 * faster to resolve than an ER node, or a Marker node.
 * <p>
 * The {@link FieldCostComparator} orders a query cased on the field cardinality. This cardinality can be gathered from the metadata table across the entire
 * date range of the query, or the cardinality can be gathered from the global index and applied on a per-shard basis.
 * <p>
 * The {@link TermCostComparator} orders a query based on the term cardinality. This is gathered from the global index and applied on a per-shard basis.
 */
public class OrderByCostVisitor extends BaseVisitor {

    private static final Logger log = Logger.getLogger(OrderByCostVisitor.class);

    private JexlNodeComparator comparator;
    private final boolean isFieldMap;
    private final Map<String,Long> countMap;

    /**
     * Wrapper method for {@link #order(ASTJexlScript)}.
     *
     * @param query
     *            a string representation of a query
     * @return a cost-ordered query
     */
    public static String order(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            script = order(script);
            return JexlStringBuildingVisitor.buildQueryWithoutParse(script);
        } catch (ParseException e) {
            log.error("Could not order query by cost: " + query, e);
        }
        return null;
    }

    /**
     * Orders the query tree by arbitrary cost
     *
     * @param script
     *            the query tree
     * @return a query tree ordered by arbitrary cost
     */
    public static ASTJexlScript order(ASTJexlScript script) {
        return order(script, null, false);
    }

    /**
     * Orders the query by field counts
     *
     * @param script
     *            the query tree
     * @param counts
     *            a map of field counts
     * @return a query tree ordered by field counts
     */
    public static ASTJexlScript orderByFieldCount(ASTJexlScript script, Map<String,Long> counts) {
        return order(script, counts, true);
    }

    /**
     * Orders the query by field counts
     *
     * @param node
     *            an arbitrary JexlNode
     * @param counts
     *            a map of field counts
     * @return a query tree ordered by field counts
     */
    public static ASTJexlScript orderByFieldCount(JexlNode node, Map<String,Long> counts) {
        return order(node, counts, true);
    }

    /**
     * Orders the query tree by term counts
     *
     * @param script
     *            the query tree
     * @param counts
     *            a map of term counts
     * @return a query tree ordered by term counts
     */
    public static ASTJexlScript orderByTermCount(ASTJexlScript script, Map<String,Long> counts) {
        return order(script, counts, false);
    }

    /**
     * Orders the query tree by term counts
     *
     * @param node
     *            an arbitrary JexlNode
     * @param counts
     *            a map of term counts
     * @return a query tree ordered by term counts
     */
    public static ASTJexlScript orderByTermCount(JexlNode node, Map<String,Long> counts) {
        return order(node, counts, false);
    }

    /**
     * Orders a query tree
     *
     * @param script
     *            the query tree
     * @param counts
     *            a map of field or term counts
     * @param isFieldMap
     *            a flag that determines how the count map is interpreted
     * @return an ordered query tree
     */
    private static ASTJexlScript order(ASTJexlScript script, Map<String,Long> counts, boolean isFieldMap) {
        OrderByCostVisitor visitor = new OrderByCostVisitor(counts, isFieldMap);
        return (ASTJexlScript) script.jjtAccept(visitor, null);
    }

    /**
     * Orders a query tree
     *
     * @param node
     *            the query tree
     * @param counts
     *            a map of field or term counts
     * @param isFieldMap
     *            a flag that determines how the count map is interpreted
     * @return an ordered query tree
     */
    private static ASTJexlScript order(JexlNode node, Map<String,Long> counts, boolean isFieldMap) {
        OrderByCostVisitor visitor = new OrderByCostVisitor(counts, isFieldMap);
        return (ASTJexlScript) node.jjtAccept(visitor, null);
    }

    public OrderByCostVisitor(Map<String,Long> counts, boolean isFieldMap) {
        this.countMap = counts;
        this.isFieldMap = isFieldMap;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return node;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitJunction(node, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitJunction(node, data);
    }

    // Do not descend into functions
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }

    private Object visitJunction(JexlNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (!instance.isAnyType()) {
            JexlNode[] children = JexlNodes.getChildren(node);
            Arrays.sort(children, getComparator());
            JexlNodes.setChildren(node, children);

            node.childrenAccept(this, data);
        }
        return data;
    }

    private JexlNodeComparator getComparator() {
        if (comparator == null) {
            if (countMap != null) {
                if (isFieldMap) {
                    comparator = new FieldCostComparator(countMap);
                } else {
                    comparator = new TermCostComparator(countMap);
                }
            } else {
                comparator = new DefaultJexlNodeComparator();
            }
        }
        return comparator;
    }

}

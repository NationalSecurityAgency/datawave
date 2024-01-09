package datawave.query.jexl.visitors.order;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.NodeCostComparator;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuilder;

/**
 * Order query nodes by cost. Default cost is assumed via {@link org.apache.commons.jexl2.parser.ParserTreeConstants}.
 *
 * An informed cost would count the occurrences of a field-value pair off the global index for a given shard.
 *
 * In general an EQ node is faster to resolve than an ER node.
 *
 * In general an ER node is faster to resolve than a function node.
 */
public class OrderByCostVisitor extends BaseVisitor {

    private static final Logger log = Logger.getLogger(OrderByCostVisitor.class);

    private final NodeCostComparator costComparator = new NodeCostComparator();

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
            log.error("Could not order query by cost: " + query);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Orders the query tree by arbitrary cost. Reuses the script object.
     *
     * @param script
     *            the query tree
     * @return a query tree ordered by arbitrary cost
     */
    public static ASTJexlScript order(ASTJexlScript script) {
        // At this point the query iterator has a binary tree. Flatten first so children of the
        // same logical expression are easier to order.
        script = TreeFlatteningRebuilder.flatten(script);

        return (ASTJexlScript) script.jjtAccept(new OrderByCostVisitor(), null);
    }

    public static ASTJexlScript order(ASTJexlScript script, Map<String,Long> counts) {
        // At this point the query iterator has a binary tree. Flatten first so children of the
        // same logical expression are easier to order.
        script = TreeFlatteningRebuilder.flatten(script);

        return (ASTJexlScript) script.jjtAccept(new OrderByCostVisitor(), null);
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return node;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {

        JexlNode[] children = JexlNodes.children(node);
        Arrays.sort(children, costComparator);
        JexlNodes.children(node, children);

        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        JexlNode[] children = JexlNodes.children(node);
        Arrays.sort(children, costComparator);
        JexlNodes.children(node, children);

        node.childrenAccept(this, data);
        return data;
    }

    // Do not descend into functions
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }

}

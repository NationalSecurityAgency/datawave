package datawave.query.jexl.visitors.validate;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.Instance;
import datawave.query.jexl.visitors.ShortCircuitBaseVisitor;

/**
 * Verifies the lower bound sorts before the upper bound for all bounded ranges.
 * <p>
 * This visitor must be run after bounded range expansion due to how normalization happens.
 */
public class ValidateBoundedRangeVisitor extends ShortCircuitBaseVisitor {

    private static final Logger log = LoggerFactory.getLogger(ValidateBoundedRangeVisitor.class);

    private ValidateBoundedRangeVisitor() {
        // enforce static access
    }

    public static <T extends JexlNode> T validate(T node) {
        ValidateBoundedRangeVisitor visitor = new ValidateBoundedRangeVisitor();
        node.jjtAccept(visitor, null);
        return node;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(BOUNDED_RANGE)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            if (range != null) {
                if (range.areBoundsEquivalent()) {
                    log.warn("lower and upper bound are equivalent, consider rewriting to a single equality node");
                } else if (range.isLowerBoundGreaterThanUpperBound()) {
                    throw new IllegalStateException("lower bound was greater than the upper bound: " + range);
                }
            }
            return data;
        }
        return super.visit(node, data);
    }

}

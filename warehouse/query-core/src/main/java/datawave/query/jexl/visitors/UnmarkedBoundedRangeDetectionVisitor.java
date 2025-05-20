package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.data.normalizer.Normalizer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.Instance;

/**
 * A visitor that detects invalid ranges as either a bounded marker with invalid source, or a valid source without a marker
 * <p>
 * Note: this visitor does NOT validate that the lower bound sorts before the upper bound due to how various {@link Normalizer}s may change the values.
 */
public class UnmarkedBoundedRangeDetectionVisitor extends BaseVisitor {

    private boolean unmarkedBoundedRangeFound = false;

    private UnmarkedBoundedRangeDetectionVisitor() {
        // enforce static access
    }

    public static boolean findUnmarkedBoundedRanges(JexlNode script) {
        UnmarkedBoundedRangeDetectionVisitor visitor = new UnmarkedBoundedRangeDetectionVisitor();
        script.jjtAccept(visitor, null);
        return visitor.foundUnmarkedBoundedRange();
    }

    public boolean foundUnmarkedBoundedRange() {
        return unmarkedBoundedRangeFound;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {

        if (unmarkedBoundedRangeFound) {
            // if we already found an unmarked bounded range there is no need to evaluate other nodes
            return data;
        }

        // check for a bounded marker where the source node is not a range
        Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(BOUNDED_RANGE)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            if (range == null) {
                unmarkedBoundedRangeFound = true;
            }
            return data;
        }

        // check for a range that is not marked
        LiteralRange<?> range = JexlASTHelper.findRange().notDelayed().notMarked().getRange(node);
        if (range != null && range.isBounded()) {
            unmarkedBoundedRangeFound = true;
            return data;
        }

        return super.visit(node, data);
    }
}

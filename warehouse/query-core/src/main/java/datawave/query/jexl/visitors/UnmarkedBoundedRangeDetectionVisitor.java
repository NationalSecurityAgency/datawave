package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.concurrent.atomic.AtomicBoolean;

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

    private UnmarkedBoundedRangeDetectionVisitor() {
        // enforce static access
    }

    public static boolean findUnmarkedBoundedRanges(JexlNode script) {
        UnmarkedBoundedRangeDetectionVisitor visitor = new UnmarkedBoundedRangeDetectionVisitor();

        AtomicBoolean unmarked = new AtomicBoolean(false);
        script.jjtAccept(visitor, unmarked);

        return unmarked.get();
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (data == null) {
            return null;
        }

        // check for a bounded marker where the source node is not a range
        Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(BOUNDED_RANGE)) {
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
            if (range == null) {
                AtomicBoolean hasBounded = (AtomicBoolean) data;
                hasBounded.set(true);
            }
            return false;
        }

        // check for a range that is not marked
        LiteralRange<?> range = JexlASTHelper.findRange().notDelayed().notMarked().getRange(node);
        if (range != null && range.isBounded()) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
            return false;
        }

        return super.visit(node, data);
    }
}

package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.concurrent.atomic.AtomicBoolean;

public class UnmarkedBoundedRangeDetectionVisitor extends BaseVisitor {
    
    public static boolean findUnmarkedBoundedRanges(JexlNode script) {
        UnmarkedBoundedRangeDetectionVisitor visitor = new UnmarkedBoundedRangeDetectionVisitor();
        
        AtomicBoolean unmarked = new AtomicBoolean(false);
        script.jjtAccept(visitor, unmarked);
        
        return unmarked.get();
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        // determine if we have a marked range that is not actually a range
        if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class)) {
            if (null != data && !JexlASTHelper.findRange().isRange(node)) {
                AtomicBoolean hasBounded = (AtomicBoolean) data;
                hasBounded.set(true);
            }
            
            return false;
        }
        // determine if we have a range that is not marked
        else if (JexlASTHelper.findRange().notDelayed().notMarked().isRange(node)) {
            if (null != data) {
                AtomicBoolean hasBounded = (AtomicBoolean) data;
                hasBounded.set(true);
            }
            
            return false;
        } else {
            return super.visit(node, data);
        }
    }
}

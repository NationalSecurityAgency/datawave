package datawave.query.rewrite.planner.pushdown;

import datawave.query.rewrite.jexl.nodes.QueryPropertyMarker;

import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

/**
 * 
 */
public class IsType implements Predicate<JexlNode> {
    
    private Class<? extends JexlNode> type;
    
    public IsType(Class<? extends JexlNode> type) {
        this.type = type;
    }
    
    @Override
    public boolean apply(JexlNode node) {
        Preconditions.checkNotNull(node);
        if (QueryPropertyMarker.class.isAssignableFrom(type)) {
            return QueryPropertyMarker.instanceOf(node, (Class<? extends QueryPropertyMarker>) type);
        } else {
            return node.getClass().isAssignableFrom(type);
        }
    }
}

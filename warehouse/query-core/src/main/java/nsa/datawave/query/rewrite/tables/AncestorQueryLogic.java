package nsa.datawave.query.rewrite.tables;

import nsa.datawave.query.rewrite.ancestor.AncestorQueryIterator;
import nsa.datawave.query.rewrite.ancestor.AncestorUidIntersector;
import nsa.datawave.query.rewrite.planner.QueryPlanner;

/**
 * AncestorQueryLogic is basically like the regular ShardQueryLogic except that the metadata from all of the ancestors up to the TLD are included. The idea is
 * that metadata for a parent is also inherited by the child.
 *
 */
public class AncestorQueryLogic extends RefactoredShardQueryLogic {
    
    public AncestorQueryLogic() {
        super();
        setUidIntersector(new AncestorUidIntersector());
        setIter();
    }
    
    public AncestorQueryLogic(AncestorQueryLogic other) {
        super(other);
        setIter();
    }
    
    @Override
    public void setQueryPlanner(QueryPlanner planner) {
        super.setQueryPlanner(planner);
        setIter();
    }
    
    private void setIter() {
        getQueryPlanner().setQueryIteratorClass(AncestorQueryIterator.class);
    }
    
    @Override
    public AncestorQueryLogic clone() {
        return new AncestorQueryLogic(this);
    }
}

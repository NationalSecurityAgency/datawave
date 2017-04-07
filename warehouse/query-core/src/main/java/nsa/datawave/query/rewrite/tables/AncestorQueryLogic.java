package nsa.datawave.query.rewrite.tables;

import nsa.datawave.query.rewrite.ancestor.AncestorQueryIterator;
import nsa.datawave.query.rewrite.ancestor.AncestorQueryPlanner;
import nsa.datawave.query.rewrite.ancestor.AncestorRangeStream;
import nsa.datawave.query.rewrite.ancestor.AncestorUidIntersector;
import nsa.datawave.query.rewrite.planner.DefaultQueryPlanner;
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
        QueryPlanner planner = new AncestorQueryPlanner();
        setQueryPlanner(planner);
        setRangeStream();
        setIter();
    }
    
    public AncestorQueryLogic(AncestorQueryLogic other) {
        super(other);
        setIter();
    }
    
    @Override
    public void setQueryPlanner(QueryPlanner planner) {
        if (!(planner instanceof DefaultQueryPlanner)) {
            throw new IllegalArgumentException("Query logic requires DefaultQueryPlanner compatibility");
        }

        super.setQueryPlanner(planner);
        setRangeStream();
        setIter();
    }
    
    /**
     * Overriding this generates TLD end ranges without generating TLD start ranges. Essentially expanding a hit down the branch so all branch candidates can be
     * potentially hit. This is specifically for the case where the index hits further up the tree than the additional/delayed predicates. Without this no range
     * will be considered for the document so hits will potentially be missed
     *
     * Specific expansion point TupleToRange.apply()
     *
     * @return always true
     */
    @Override
    public boolean isTldQuery() {
        return true;
    }
    
    private void setRangeStream() {
        ((DefaultQueryPlanner) getQueryPlanner()).setRangeStreamClass(AncestorRangeStream.class.getCanonicalName());
    }
    
    private void setIter() {
        getQueryPlanner().setQueryIteratorClass(AncestorQueryIterator.class);
    }
    
    @Override
    public AncestorQueryLogic clone() {
        return new AncestorQueryLogic(this);
    }
}

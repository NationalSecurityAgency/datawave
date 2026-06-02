package datawave.query.tables;

import datawave.query.ancestor.AncestorQueryIterator;
import datawave.query.ancestor.AncestorQueryPlanner;
import datawave.query.ancestor.AncestorRangeStream;
import datawave.query.ancestor.AncestorUidIntersector;
import datawave.query.index.lookup.AncestorCreateUidsIterator;
import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlanner;

/**
 * AncestorQueryLogic is basically like the regular ShardQueryLogic except that the metadata from all of the ancestors up to the TLD are included. The idea is
 * that metadata for a parent is also inherited by the child.
 *
 */
public class AncestorQueryLogic extends ShardQueryLogic {
    public AncestorQueryLogic() {
        super();
        setIsTldQuery(true);
        setCreateUidsIteratorClass(AncestorCreateUidsIterator.class);
        setUidIntersector(new AncestorUidIntersector());
        QueryPlanner planner = new AncestorQueryPlanner();
        setQueryPlanner(planner);
        setRangeStream();
        setIter();
    }

    public AncestorQueryLogic(AncestorQueryLogic other) {
        super(other);
        setIsTldQuery(true);
        setIter();
    }

    @Override
    public void setQueryPlanner(QueryPlanner planner) {
        if (!(planner instanceof DefaultQueryPlanner) && !(planner instanceof DatePartitionedQueryPlanner)) {
            throw new IllegalArgumentException("Query logic requires DefaultQueryPlanner or FederatedQueryPlanner compatibility");
        }

        super.setQueryPlanner(planner);
        setRangeStream();
        setIter();
    }

    private DefaultQueryPlanner getDefaultQueryPlanner() {
        QueryPlanner planner = getQueryPlanner();
        if (planner instanceof DatePartitionedQueryPlanner) {
            return ((DatePartitionedQueryPlanner) planner).getQueryPlanner();
        } else {
            return (DefaultQueryPlanner) planner;
        }
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
        getDefaultQueryPlanner().setRangeStreamClass(AncestorRangeStream.class.getCanonicalName());
    }

    private void setIter() {
        getQueryPlanner().setQueryIteratorClass(AncestorQueryIterator.class);
    }

    @Override
    public AncestorQueryLogic clone() {
        return new AncestorQueryLogic(this);
    }
}

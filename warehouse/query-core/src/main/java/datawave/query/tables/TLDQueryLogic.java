package datawave.query.tables;

import datawave.query.planner.QueryPlanner;
import datawave.query.tld.CreateTLDUidsIterator;
import datawave.query.tld.TLDQueryIterator;

/**
 * TLDQueryLogic is mostly the regular query with a special query iterator...
 *
 * 
 *
 */
public class TLDQueryLogic extends ShardQueryLogic {
    
    public TLDQueryLogic() {
        super();
        setCreateUidsIteratorClass(CreateTLDUidsIterator.class);
        setIter();
    }
    
    public TLDQueryLogic(TLDQueryLogic other) {
        super(other);
        setIter();
    }
    
    @Override
    public void setQueryPlanner(QueryPlanner planner) {
        super.setQueryPlanner(planner);
        setIter();
    }
    
    private void setIter() {
        getQueryPlanner().setQueryIteratorClass(TLDQueryIterator.class);
    }
    
    @Override
    public TLDQueryLogic clone() {
        return new TLDQueryLogic(this);
    }
}

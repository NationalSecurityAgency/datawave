package datawave.query.rewrite.tables;

import datawave.query.rewrite.planner.QueryPlanner;
import datawave.query.rewrite.tld.CreateTLDUidsIterator;
import datawave.query.rewrite.tld.TLDQueryIterator;

/**
 * TLDQueryLogic is mostly the regular query with a special query iterator...
 *
 * 
 *
 */
public class RefactoredTLDQueryLogic extends RefactoredShardQueryLogic {
    
    public RefactoredTLDQueryLogic() {
        super();
        setCreateUidsIteratorClass(CreateTLDUidsIterator.class);
        setIter();
    }
    
    public RefactoredTLDQueryLogic(RefactoredTLDQueryLogic other) {
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
    public RefactoredTLDQueryLogic clone() {
        return new RefactoredTLDQueryLogic(this);
    }
}

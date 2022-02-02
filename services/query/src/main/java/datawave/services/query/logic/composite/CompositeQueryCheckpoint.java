package datawave.services.query.logic.composite;

import datawave.services.query.logic.QueryCheckpoint;

public class CompositeQueryCheckpoint extends QueryCheckpoint {
    protected String delegateQueryLogic;
    
    public CompositeQueryCheckpoint(String delegateQueryLogic, QueryCheckpoint checkpoint) {
        super(checkpoint);
        this.delegateQueryLogic = delegateQueryLogic;
    }
    
    public String getDelegateQueryLogic() {
        return delegateQueryLogic;
    }
    
    public void setDelegateQueryLogic(String delegateQueryLogic) {
        this.delegateQueryLogic = delegateQueryLogic;
    }
}

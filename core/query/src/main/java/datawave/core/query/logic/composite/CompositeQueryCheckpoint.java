package datawave.core.query.logic.composite;

import java.io.Serializable;

import datawave.core.query.logic.QueryCheckpoint;

public class CompositeQueryCheckpoint extends QueryCheckpoint implements Serializable {
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

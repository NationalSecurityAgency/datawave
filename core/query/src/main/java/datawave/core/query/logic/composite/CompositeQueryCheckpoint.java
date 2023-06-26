package datawave.core.query.logic.composite;

import datawave.core.query.logic.QueryCheckpoint;

import java.io.Serializable;

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

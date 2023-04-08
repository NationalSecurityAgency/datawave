package datawave.core.query.remote;

import datawave.core.query.logic.QueryLogic;
import datawave.security.authorization.UserOperations;

public interface RemoteQueryLogic<T> extends QueryLogic<T> {
    void setRemoteQueryService(RemoteQueryService service);
    
    void setUserOperations(UserOperations userOperations);
}

package datawave.query.tables.remote;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.remote.RemoteQueryService;
import datawave.security.authorization.UserOperations;

/**
 * A remote query logic is is a query logic that uses a remote query service.
 */
public interface RemoteQueryLogic<T> extends QueryLogic<T> {
    public void setRemoteQueryService(RemoteQueryService service);
    
    public void setUserOperations(UserOperations userOperations);
}

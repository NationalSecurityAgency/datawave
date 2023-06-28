package datawave.query.tables.remote;

import datawave.security.authorization.UserOperations;
import datawave.webservice.common.remote.RemoteQueryService;
import datawave.webservice.query.logic.QueryLogic;

/**
 * A remote query logic is is a query logic that uses a remote query service.
 */
public interface RemoteQueryLogic<T> extends QueryLogic<T> {
    public void setRemoteQueryService(RemoteQueryService service);

    public void setUserOperations(UserOperations userOperations);
}

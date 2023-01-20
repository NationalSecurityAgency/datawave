package datawave.core.query.remote;

import datawave.core.query.logic.QueryLogic;

public interface RemoteQueryLogic<T> extends QueryLogic<T> {
    public void setRemoteQueryService(RemoteQueryService service);
}

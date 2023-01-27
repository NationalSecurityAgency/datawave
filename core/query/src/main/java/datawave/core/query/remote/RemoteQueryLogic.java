package datawave.core.query.remote;

import datawave.core.query.logic.QueryLogic;

public interface RemoteQueryLogic<T> extends QueryLogic<T> {
    void setRemoteQueryService(RemoteQueryService service);
}

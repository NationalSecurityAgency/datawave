package datawave.microservice.query.remote;

import static datawave.microservice.query.remote.QueryRequest.Method.CANCEL;
import static datawave.microservice.query.remote.QueryRequest.Method.CLOSE;

public class QueryRequest {
    
    public enum Method {
        CANCEL, CLOSE
    }
    
    private final Method method;
    private final String queryId;
    
    private QueryRequest(Method method) {
        this(method, null);
    }
    
    private QueryRequest(Method method, String queryId) {
        this.method = method;
        this.queryId = queryId;
    }
    
    public Method getMethod() {
        return method;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    @Override
    public String toString() {
        return "Remote Request: method=" + method + ", queryId=" + queryId;
    }
    
    public static QueryRequest cancel(String queryId) {
        return new QueryRequest(CANCEL, queryId);
    }
    
    public static QueryRequest close(String queryId) {
        return new QueryRequest(CLOSE, queryId);
    }
}

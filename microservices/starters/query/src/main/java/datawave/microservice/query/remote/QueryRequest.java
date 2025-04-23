package datawave.microservice.query.remote;

import static datawave.microservice.query.remote.QueryRequest.Method.CANCEL;
import static datawave.microservice.query.remote.QueryRequest.Method.CLOSE;
import static datawave.microservice.query.remote.QueryRequest.Method.CREATE;
import static datawave.microservice.query.remote.QueryRequest.Method.NEXT;
import static datawave.microservice.query.remote.QueryRequest.Method.PLAN;
import static datawave.microservice.query.remote.QueryRequest.Method.PREDICT;

public class QueryRequest {
    
    public enum Method {
        CREATE, PLAN, PREDICT, NEXT, CANCEL, CLOSE
    }
    
    private final Method method;
    private final String queryId;
    
    private QueryRequest() {
        this(null, null);
    }
    
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
    
    public static QueryRequest create(String queryId) {
        return new QueryRequest(CREATE, queryId);
    }
    
    public static QueryRequest next(String queryId) {
        return new QueryRequest(NEXT, queryId);
    }
    
    public static QueryRequest cancel(String queryId) {
        return new QueryRequest(CANCEL, queryId);
    }
    
    public static QueryRequest close(String queryId) {
        return new QueryRequest(CLOSE, queryId);
    }
    
    public static QueryRequest plan(String queryId) {
        return new QueryRequest(PLAN, queryId);
    }
    
    public static QueryRequest predict(String queryId) {
        return new QueryRequest(PREDICT, queryId);
    }
    
    public static QueryRequest request(Method method, String queryId) {
        return new QueryRequest(method, queryId);
    }
}

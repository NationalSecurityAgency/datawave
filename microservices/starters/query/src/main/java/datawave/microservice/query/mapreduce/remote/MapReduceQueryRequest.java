package datawave.microservice.query.mapreduce.remote;

import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.OOZIE_SUBMIT;
import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.SUBMIT;

public class MapReduceQueryRequest {
    
    public enum Method {
        SUBMIT, OOZIE_SUBMIT
    }
    
    private final Method method;
    private final String id;
    
    private MapReduceQueryRequest() {
        this(null, null);
    }
    
    private MapReduceQueryRequest(Method method) {
        this(method, null);
    }
    
    private MapReduceQueryRequest(Method method, String id) {
        this.method = method;
        this.id = id;
    }
    
    public Method getMethod() {
        return method;
    }
    
    public String getId() {
        return id;
    }
    
    @Override
    public String toString() {
        return "Remote Request: method=" + method + ", id=" + id;
    }
    
    public static MapReduceQueryRequest submit(String id) {
        return new MapReduceQueryRequest(SUBMIT, id);
    }
    
    public static MapReduceQueryRequest oozieSubmit(String id) {
        return new MapReduceQueryRequest(OOZIE_SUBMIT, id);
    }
}

package datawave.microservice.audit.replay.remote;

import static datawave.microservice.audit.replay.remote.Request.Method.STOP;
import static datawave.microservice.audit.replay.remote.Request.Method.STOP_ALL;
import static datawave.microservice.audit.replay.remote.Request.Method.UPDATE;
import static datawave.microservice.audit.replay.remote.Request.Method.UPDATE_ALL;

/**
 * Represents a remote request which can be sent out to instruct other audit service instances to update or stop their audit replays.
 */
public class Request {
    
    public enum Method {
        UPDATE, UPDATE_ALL, STOP, STOP_ALL
    }
    
    private final Method method;
    private final String id;
    
    private Request(Method method) {
        this(method, null);
    }
    
    private Request(Method method, String id) {
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
    
    public static class UpdateRequest extends Request {
        final private long sendRate;
        
        private UpdateRequest(Method method, String id, long sendRate) {
            super(method, id);
            this.sendRate = sendRate;
        }
        
        public long getSendRate() {
            return sendRate;
        }
        
        @Override
        public String toString() {
            return super.toString() + ", sendRate=" + sendRate;
        }
    }
    
    public static Request update(String id, long sendRate) {
        return new UpdateRequest(UPDATE, id, sendRate);
    }
    
    public static Request updateAll(long sendRate) {
        return new UpdateRequest(UPDATE_ALL, null, sendRate);
    }
    
    public static Request stop(String id) {
        return new Request(STOP, id);
    }
    
    public static Request stopAll() {
        return new Request(STOP_ALL);
    }
}

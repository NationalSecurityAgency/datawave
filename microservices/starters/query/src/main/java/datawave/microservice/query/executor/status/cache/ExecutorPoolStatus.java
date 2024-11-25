package datawave.microservice.query.executor.status.cache;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExecutorPoolStatus implements Serializable {
    private static final long serialVersionUID = -2210589277081455998L;
    
    private String poolName;
    
    private Map<String,Long> executorHeartbeat = new LinkedHashMap<>();
    private Map<String,Integer> queryCountByConnectionPool = new LinkedHashMap<>();
    
    public ExecutorPoolStatus(String poolName) {
        this.poolName = poolName;
    }
    
    public String getPoolName() {
        return poolName;
    }
    
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }
    
    public Map<String,Long> getExecutorHeartbeat() {
        return executorHeartbeat;
    }
    
    public void setExecutorHeartbeat(Map<String,Long> executorHeartbeat) {
        this.executorHeartbeat = executorHeartbeat;
    }
    
    public Map<String,Integer> getQueryCountByConnectionPool() {
        return queryCountByConnectionPool;
    }
    
    public void setQueryCountByConnectionPool(Map<String,Integer> queryCountByConnectionPool) {
        this.queryCountByConnectionPool = queryCountByConnectionPool;
    }
}

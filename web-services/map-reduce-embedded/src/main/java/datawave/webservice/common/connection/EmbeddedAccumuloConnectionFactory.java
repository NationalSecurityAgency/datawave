package datawave.webservice.common.connection;

import org.apache.accumulo.core.client.Connector;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedAccumuloConnectionFactory implements AccumuloConnectionFactory {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @ConfigProperty(name = "dw.warehouse.zookeepers")
    private String zookeepers;
    
    @ConfigProperty(name = "dw.warehouse.instanceName")
    private String instanceName;
    
    @ConfigProperty(name = "dw.warehouse.userName")
    private String userName;
    
    @ConfigProperty(name = "dw.warehouse.password")
    private String password;
    
    private AccumuloConnectionPool pool;
    
    @PostConstruct
    private void initialize() {
        pool = createConnectionPool(10);
    }
    
    private AccumuloConnectionPool createConnectionPool(int limit) {
        AccumuloConnectionPoolFactory factory = new AccumuloConnectionPoolFactory(this.userName, this.password, this.zookeepers, this.instanceName);
        AccumuloConnectionPool pool = new AccumuloConnectionPool(factory);
        pool.setTestOnBorrow(true);
        pool.setTestOnReturn(true);
        pool.setMaxTotal(limit);
        pool.setMaxIdle(-1);
        
        try {
            pool.addObject();
        } catch (Exception e) {
            log.error("Error pre-populating connection pool", e);
        }
        
        return pool;
    }
    
    @Override
    public String getConnectionUserName(String poolName) {
        return this.userName;
    }
    
    @Override
    public Connector getConnection(Priority priority, Map<String,String> trackingMap) throws Exception {
        return pool.borrowObject(trackingMap);
    }
    
    @Override
    public Connector getConnection(String poolName, Priority priority, Map<String,String> trackingMap) throws Exception {
        return pool.borrowObject(trackingMap);
    }
    
    @Override
    public void returnConnection(Connector connection) throws Exception {
        pool.returnObject(connection);
    }
    
    @Override
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        HashMap<String,String> trackingMap = new HashMap<>();
        if (stackTrace != null) {
            StackTraceElement ste = stackTrace[1];
            trackingMap.put("request.location", ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber());
        }
        
        return trackingMap;
    }
}

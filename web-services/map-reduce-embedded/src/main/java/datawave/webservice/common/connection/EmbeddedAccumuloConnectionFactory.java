package datawave.webservice.common.connection;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    
    private AccumuloClientPool pool;
    
    @PostConstruct
    private void initialize() {
        pool = createClientPool(10);
    }
    
    private AccumuloClientPool createClientPool(int limit) {
        AccumuloClientPoolFactory factory = new AccumuloClientPoolFactory(this.userName, this.password, this.zookeepers, this.instanceName);
        AccumuloClientPool pool = new AccumuloClientPool(factory);
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
    public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority, Map<String,String> trackingMap) throws Exception {
        return pool.borrowObject(trackingMap);
    }
    
    @Override
    public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority, Map<String,String> trackingMap)
                    throws Exception {
        return pool.borrowObject(trackingMap);
    }
    
    @Override
    public void returnClient(AccumuloClient client) throws Exception {
        pool.returnObject(client);
    }
    
    @Override
    public String report() {
        return pool.toString();
    }
    
    @Override
    public List<ConnectionPool> getConnectionPools() {
        return Collections.EMPTY_LIST;
    }
    
    @Override
    public int getConnectionUsagePercent() {
        return 0;
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
    
    @Override
    public void close() throws Exception {
        pool.close();
    }
}

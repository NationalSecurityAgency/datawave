package nsa.datawave.webservice.common.connection;

import java.util.Map;

import org.apache.accumulo.core.client.Connector;

public interface AccumuloConnectionFactory {
    
    public enum Priority {
        
        LOW, NORMAL, HIGH, ADMIN
    }
    
    public enum State {
        
        WAITING, CONNECTED
    }
    
    /**
     * Deprecated in 2.2.3, use getConnectionUserName(String poolName)
     *
     * @return name of the user used in the connection pools
     */
    @Deprecated
    public String getConnectionUserName();
    
    /**
     * @param poolName
     *            the name of the pool to query
     * @return name of the user used in the connection pools
     */
    public String getConnectionUserName(String poolName);
    
    /**
     * Gets a connection from the pool with the assigned priority
     *
     * Deprecated in 2.2.3, use getConnection(String poolName, Priority priority, Map<String, String> trackingMap)
     *
     * @param priority
     *            the connection's Priority
     * @return accumulo connection
     * @throws Exception
     */
    public Connector getConnection(Priority priority, Map<String,String> trackingMap) throws Exception;
    
    /**
     * Gets a connection from the named pool with the assigned priority
     *
     * @param poolName
     *            the name of the pool to retrieve the connection from
     * @param priority
     *            the priority of the connection
     * @param trackingMap
     *            the tracking map
     * @return Accumulo connection
     * @throws Exception
     */
    public Connector getConnection(String poolName, Priority priority, Map<String,String> trackingMap) throws Exception;
    
    /**
     * Returns the connection to the pool with the associated priority.
     *
     * @param connection
     *            The connection to return
     * @throws Exception
     */
    public void returnConnection(Connector connection) throws Exception;
    
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace);
}

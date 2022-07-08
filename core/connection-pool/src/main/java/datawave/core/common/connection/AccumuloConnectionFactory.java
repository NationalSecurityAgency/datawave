package datawave.core.common.connection;

import datawave.core.common.result.ConnectionPool;
import org.apache.accumulo.core.client.Connector;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface AccumuloConnectionFactory extends AutoCloseable {
    
    String USER_DN = "user.dn";
    String PROXY_SERVERS = "proxyServers";
    String REQUEST_LOCATION = "request.location";
    String START_TIME = "connection.state.start";
    String STATE = "state";
    String THREAD_NAME = "thread.name";
    String QUERY_USER = "query.user";
    String QUERY_ID = "query.id";
    String QUERY = "query.query";
    
    enum Priority {
        
        LOW, NORMAL, HIGH, ADMIN
    }
    
    enum State {
        
        WAITING, CONNECTED
    }
    
    /**
     * Gets a connection from the pool with the assigned priority
     *
     * Deprecated in 2.2.3, use {@link #getConnection(String, Collection, String, Priority, Map)}
     *
     * @param priority
     *            the connection's Priority
     * @param trackingMap
     *            the tracking map
     * @return accumulo connection
     * @throws Exception
     *             on failure
     */
    Connector getConnection(String userDN, Collection<String> proxyServers, Priority priority, Map<String,String> trackingMap) throws Exception;
    
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
     *             on failure
     */
    Connector getConnection(String userDN, Collection<String> proxyServers, String poolName, Priority priority, Map<String,String> trackingMap)
                    throws Exception;
    
    /**
     * Returns the connection to the pool with the associated priority.
     *
     * @param connection
     *            The connection to return
     * @throws Exception
     *             on failure
     */
    void returnConnection(Connector connection) throws Exception;
    
    /**
     * Return a report of the current connection factory usage
     */
    String report();
    
    /**
     * Get a description of the current pools
     * 
     * @return A list of connection pools
     */
    List<ConnectionPool> getConnectionPools();
    
    /**
     * Get the current connection usage percentage. This can be used for balancing purposes.
     * 
     * @return The usage percentage (0 - 100)
     */
    int getConnectionUsagePercent();
    
    /**
     * Get a tracking map to be used in the getConnection calls
     *
     * @param stackTrace
     *            The callers stack trace
     * @return A map representation
     */
    Map<String,String> getTrackingMap(StackTraceElement[] stackTrace);
    
}

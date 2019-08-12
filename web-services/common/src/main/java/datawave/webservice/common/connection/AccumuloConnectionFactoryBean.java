package datawave.webservice.common.connection;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.config.ConnectionPoolConfiguration;
import datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;
import datawave.webservice.common.result.Connection;
import datawave.webservice.common.result.ConnectionFactoryResponse;
import datawave.webservice.common.result.ConnectionPool;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.tracer.AsyncSpanReceiver;
import org.apache.accumulo.tracer.ZooTraceClient;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Trace;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBContext;
import javax.ejb.Local;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Path("/Common/AccumuloConnectionFactory")
@Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
@GZIP
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Local(AccumuloConnectionFactory.class)
// declare a local EJB interface for users of this bean (@LocalBean would otherwise prevent using the interface)
@LocalBean
// declare a no-interface view so the JAX-RS annotations are honored for the singleton (otherwise a new instance will be created per-request)
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
// transactions not supported directly by this bean
@Lock(LockType.READ)
@MBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class AccumuloConnectionFactoryBean implements AccumuloConnectionFactory {
    public static final String TRACE_HOST_PROPERTY = "trace.host";
    public static final String TRACE_SERVICE_PROPERTY = "trace.service";
    public static final String TRACER_ZK_HOST = "tracer.zookeeper.host";
    public static final String TRACER_ZK_PATH = "tracer.zookeeper.path";
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Resource
    private EJBContext context;
    
    @Inject
    private AccumuloTableCache cache;
    
    private Map<String,Map<Priority,AccumuloConnectionPool>> pools;
    
    @Inject
    private ConnectionPoolsConfiguration connectionPoolsConfiguration;
    
    private String defaultPoolName = null;
    
    @PostConstruct
    public void init() {
        this.pools = new HashMap<>();
        
        if (this.connectionPoolsConfiguration == null) {
            log.error("connectionPoolsConfiguration was null - aborting init()");
            return;
        }
        HashMap<String,Pair<String,PasswordToken>> instances = new HashMap<>();
        this.defaultPoolName = connectionPoolsConfiguration.getDefaultPool();
        for (Entry<String,ConnectionPoolConfiguration> entry : connectionPoolsConfiguration.getPools().entrySet()) {
            Map<Priority,AccumuloConnectionPool> p = new HashMap<>();
            ConnectionPoolConfiguration conf = entry.getValue();
            p.put(Priority.ADMIN, createConnectionPool(conf, conf.getAdminPriorityPoolSize()));
            p.put(Priority.HIGH, createConnectionPool(conf, conf.getHighPriorityPoolSize()));
            p.put(Priority.NORMAL, createConnectionPool(conf, conf.getNormalPriorityPoolSize()));
            p.put(Priority.LOW, createConnectionPool(conf, conf.getLowPriorityPoolSize()));
            this.pools.put(entry.getKey(), Collections.unmodifiableMap(p));
            try {
                setupMockAccumuloUser(conf, p.get(Priority.NORMAL), instances);
            } catch (Exception e) {
                log.error("Error configuring mock accumulo user for AccumuloConnectionFactoryBean.", e);
            }
            
            // Initialize the distributed tracing system. This needs to be done once at application startup. Since
            // it is tied to Accumulo connections, we do it here in this singleton bean.
            String appName = "datawave_ws";
            try {
                appName = System.getProperty("app", "datawave_ws");
            } catch (SecurityException e) {
                log.warn("Unable to retrieve system property \"app\": " + e.getMessage());
            }
            try {
                ClientConfiguration zkConfig = ClientConfiguration.loadDefault().withInstance(entry.getValue().getInstance())
                                .withZkHosts(entry.getValue().getZookeepers());
                ZooKeeperInstance instance = new ZooKeeperInstance(zkConfig);
                Map<String,String> confMap = new HashMap<>();
                confMap.put(TRACER_ZK_HOST, instance.getZooKeepers());
                confMap.put(TRACER_ZK_PATH, Constants.ZTRACERS);
                confMap.put(TRACE_HOST_PROPERTY, InetAddress.getLocalHost().getHostName());
                confMap.put(TRACE_SERVICE_PROPERTY, appName);
                confMap.put(AsyncSpanReceiver.SEND_TIMER_MILLIS, "1000");
                confMap.put(AsyncSpanReceiver.QUEUE_SIZE, "5000");
                Trace.addReceiver(new ZooTraceClient(HTraceConfiguration.fromMap(confMap)));
            } catch (IOException e) {
                log.error("Unable to initialize distributed tracing system: " + e.getMessage(), e);
            }
        }
        
        cache.setConnectionFactory(this);
    }
    
    private AccumuloConnectionPool createConnectionPool(ConnectionPoolConfiguration conf, int limit) {
        AccumuloConnectionPoolFactory factory = new AccumuloConnectionPoolFactory(conf.getUsername(), conf.getPassword(), conf.getZookeepers(),
                        conf.getInstance());
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
    
    private void setupMockAccumuloUser(ConnectionPoolConfiguration conf, AccumuloConnectionPool pool, HashMap<String,Pair<String,PasswordToken>> instances)
                    throws Exception {
        Connector c = null;
        try {
            c = pool.borrowObject(new HashMap<>());
            
            Pair<String,PasswordToken> pair = instances.get(cache.getInstance().getInstanceID());
            String user = "root";
            PasswordToken password = new PasswordToken(new byte[0]);
            if (pair != null && user.equals(pair.getFirst()))
                password = pair.getSecond();
            SecurityOperations security = cache.getInstance().getConnector(user, password).securityOperations();
            Set<String> users = security.listLocalUsers();
            if (!users.contains(conf.getUsername())) {
                security.createLocalUser(conf.getUsername(), new PasswordToken(conf.getPassword()));
                security.changeUserAuthorizations(conf.getUsername(), c.securityOperations().getUserAuthorizations(conf.getUsername()));
            } else {
                PasswordToken newPassword = new PasswordToken(conf.getPassword());
                // If we're changing root's password, and trying to change then keep track of that. If we have multiple instances
                // that specify mismatching passwords, then throw an error.
                if (user.equals(conf.getUsername())) {
                    if (pair != null && !newPassword.equals(pair.getSecond()))
                        throw new IllegalStateException(
                                        "Invalid AccumuloConnectionFactoryBean configuration--multiple pools are configured with different root passwords!");
                    instances.put(cache.getInstance().getInstanceID(), new Pair<>(conf.getUsername(), newPassword));
                }
                // match root's password on mock to the password on the actual Accumulo instance
                security.changeLocalUserPassword(conf.getUsername(), newPassword);
            }
        } finally {
            pool.returnObject(c);
        }
    }
    
    @PreDestroy
    public void tearDown() {
        for (Entry<String,Map<Priority,AccumuloConnectionPool>> entry : this.pools.entrySet()) {
            for (Entry<Priority,AccumuloConnectionPool> poolEntry : entry.getValue().entrySet()) {
                try {
                    poolEntry.getValue().close();
                } catch (Exception e) {
                    log.error("Error closing Accumulo Connection Pool: " + e);
                }
            }
        }
    }
    
    /**
     * @param poolName
     *            the name of the pool to query
     * @return name of the user used in the connection pools
     */
    public String getConnectionUserName(String poolName) {
        return connectionPoolsConfiguration.getPools().get(poolName).getUsername();
    }
    
    /**
     * Gets a connection from the pool with the assigned priority
     *
     * Deprecated in 2.2.3, use getConnection(String poolName, Priority priority, {@code Map<String, String> trackingMap)}
     *
     * @param priority
     *            the connection's Priority
     * @return accumulo connection
     * @throws Exception
     */
    public Connector getConnection(Priority priority, Map<String,String> trackingMap) throws Exception {
        return getConnection(null, priority, trackingMap);
    }
    
    /**
     * Gets a connection from the named pool with the assigned priority
     *
     * @param cpn
     *            the name of the pool to retrieve the connection from
     * @param priority
     *            the priority of the connection
     * @param tm
     *            the tracking map
     * @return Accumulo connection
     * @throws Exception
     */
    public Connector getConnection(final String cpn, final Priority priority, final Map<String,String> tm) throws Exception {
        final Map<String,String> trackingMap = (tm != null) ? tm : new HashMap<>();
        final String poolName = (cpn != null) ? cpn : defaultPoolName;
        
        if (!priority.equals(Priority.ADMIN)) {
            final String userDN = getCurrentUserDN();
            if (userDN != null)
                trackingMap.put("user.dn", userDN);
            
            final Collection<String> proxyServers = getCurrentProxyServers();
            if (proxyServers != null)
                trackingMap.put("proxyServers", proxyServers.toString());
        }
        AccumuloConnectionPool pool = pools.get(poolName).get(priority);
        Connector c = pool.borrowObject(trackingMap);
        Connector mock = cache.getInstance().getConnector(pool.getFactory().getUsername(), new PasswordToken(pool.getFactory().getPassword()));
        WrappedConnector wrappedConnector = new WrappedConnector(c, mock);
        String classLoaderContext = System.getProperty("dw.accumulo.classLoader.context");
        if (classLoaderContext != null) {
            wrappedConnector.setScannerClassLoaderContext(classLoaderContext);
        }
        String timeout = System.getProperty("dw.accumulo.scan.batch.timeout.seconds");
        if (timeout != null) {
            wrappedConnector.setScanBatchTimeoutSeconds(Long.parseLong(timeout));
        }
        return wrappedConnector;
    }
    
    /**
     * Returns the connection to the pool with the associated priority.
     *
     * @param connection
     *            The connection to return
     * @throws Exception
     */
    @PermitAll
    // permit anyone to return a connection
    public void returnConnection(Connector connection) throws Exception {
        if (connection instanceof WrappedConnector) {
            WrappedConnector wrappedConnector = (WrappedConnector) connection;
            wrappedConnector.clearScannerClassLoaderContext();
            connection = wrappedConnector.getReal();
        }
        for (Entry<String,Map<Priority,AccumuloConnectionPool>> entry : this.pools.entrySet()) {
            for (Entry<Priority,AccumuloConnectionPool> poolEntry : entry.getValue().entrySet()) {
                if (poolEntry.getValue().connectorCameFromHere(connection)) {
                    poolEntry.getValue().returnObject(connection);
                    return;
                }
            }
        }
        log.info("returnConnection called with connection that did not come from any AccumuloConnectionPool");
    }
    
    @PermitAll
    // permit anyone to get the report
    @JmxManaged
    public String report() {
        StringBuilder buf = new StringBuilder();
        for (Entry<String,Map<Priority,AccumuloConnectionPool>> entry : this.pools.entrySet()) {
            buf.append("**** ").append(entry.getKey()).append(" ****\n");
            buf.append("ADMIN: ").append(entry.getValue().get(Priority.ADMIN)).append("\n");
            buf.append("HIGH: ").append(entry.getValue().get(Priority.HIGH)).append("\n");
            buf.append("NORMAL: ").append(entry.getValue().get(Priority.NORMAL)).append("\n");
            buf.append("LOW: ").append(entry.getValue().get(Priority.LOW)).append("\n");
        }
        
        return buf.toString();
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns metrics for the AccumuloConnectionFactoryBean
     *
     * @return datawave.webservice.common.ConnectionFactoryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/stats")
    @RolesAllowed({"Administrator", "JBossAdministrator", "InternalUser"})
    public ConnectionFactoryResponse getConnectionFactoryMetrics() {
        ConnectionFactoryResponse response = new ConnectionFactoryResponse();
        ArrayList<ConnectionPool> connectionPools = new ArrayList<>();
        
        Set<String> exclude = new HashSet<>();
        exclude.add("connection.state.start");
        exclude.add("state");
        exclude.add("request.location");
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        
        for (Entry<String,Map<Priority,AccumuloConnectionPool>> entry : this.pools.entrySet()) {
            for (Entry<Priority,AccumuloConnectionPool> entry2 : entry.getValue().entrySet()) {
                String poolName = entry.getKey();
                Priority priority = entry2.getKey();
                AccumuloConnectionPool p = entry2.getValue();
                
                Long now = System.currentTimeMillis();
                MutableInt maxActive = new MutableInt();
                MutableInt numActive = new MutableInt();
                MutableInt maxIdle = new MutableInt();
                MutableInt numIdle = new MutableInt();
                MutableInt numWaiting = new MutableInt();
                // getConnectionPoolStats will collect the tracking maps and maxActive, numActive, maxIdle, numIdle while synchronized
                // to ensure consistency between the GenericObjectPool and the tracking maps
                List<Map<String,String>> requestingConnectionsMap = p.getConnectionPoolStats(maxActive, numActive, maxIdle, numIdle, numWaiting);
                
                ConnectionPool poolInfo = new ConnectionPool();
                poolInfo.setPriority(priority.name());
                poolInfo.setMaxActive(maxActive.toInteger());
                poolInfo.setNumActive(numActive.toInteger());
                poolInfo.setNumWaiting(numWaiting.toInteger());
                poolInfo.setMaxIdle(maxIdle.toInteger());
                poolInfo.setNumIdle(numIdle.toInteger());
                poolInfo.setPoolName(poolName);
                
                List<Connection> requestingConnections = new ArrayList<>();
                for (Map<String,String> m : requestingConnectionsMap) {
                    Connection c = new Connection();
                    String state = m.get("state");
                    if (state != null) {
                        c.setState(state);
                    }
                    String requestLocation = m.get("request.location");
                    if (requestLocation != null) {
                        c.setRequestLocation(requestLocation);
                    }
                    String stateStart = m.get("connection.state.start");
                    if (stateStart != null) {
                        Long stateStartLong = Long.valueOf(stateStart);
                        c.setTimeInState((now - stateStartLong));
                        Date stateStartDate = new Date(stateStartLong);
                        c.addProperty("connection.state.start", sdf.format(stateStartDate));
                    }
                    for (Map.Entry<String,String> e : m.entrySet()) {
                        if (!exclude.contains(e.getKey())) {
                            c.addProperty(e.getKey(), e.getValue());
                        }
                    }
                    requestingConnections.add(c);
                }
                Collections.sort(requestingConnections);
                poolInfo.setConnectionRequests(requestingConnections);
                connectionPools.add(poolInfo);
            }
        }
        response.setConnectionPools(connectionPools);
        return response;
    }
    
    @PermitAll
    @JmxManaged
    public int getConnectionUsagePercent() {
        double maxPercentage = 0.0;
        for (Entry<String,Map<Priority,AccumuloConnectionPool>> entry : pools.entrySet()) {
            for (Entry<Priority,AccumuloConnectionPool> poolEntry : entry.getValue().entrySet()) {
                // Don't include ADMIN priority connections when computing a usage percentage
                if (Priority.ADMIN.equals(poolEntry.getKey()))
                    continue;
                
                MutableInt maxActive = new MutableInt();
                MutableInt numActive = new MutableInt();
                MutableInt numWaiting = new MutableInt();
                MutableInt unused = new MutableInt();
                poolEntry.getValue().getConnectionPoolStats(maxActive, numActive, unused, unused, numWaiting);
                
                double percentage = (numActive.doubleValue() + numWaiting.doubleValue()) / maxActive.doubleValue();
                if (percentage > maxPercentage) {
                    maxPercentage = percentage;
                }
            }
        }
        return (int) (maxPercentage * 100);
    }
    
    @Override
    @PermitAll
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        HashMap<String,String> trackingMap = new HashMap<>();
        if (stackTrace != null) {
            StackTraceElement ste = stackTrace[1];
            trackingMap.put("request.location", ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber());
        }
        
        return trackingMap;
    }
    
    public String getCurrentUserDN() {
        
        String currentUserDN = null;
        Principal p = context.getCallerPrincipal();
        
        if (p != null && p instanceof DatawavePrincipal) {
            currentUserDN = ((DatawavePrincipal) p).getUserDN().subjectDN();
        }
        
        return currentUserDN;
    }
    
    public Collection<String> getCurrentProxyServers() {
        Set<String> currentProxyServers = null;
        Principal p = context.getCallerPrincipal();
        
        if (p != null && p instanceof DatawavePrincipal) {
            currentProxyServers = ((DatawavePrincipal) p).getProxyServers();
        }
        
        return currentProxyServers;
    }
}

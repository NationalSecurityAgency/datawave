package datawave.core.common.connection;

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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.result.Connection;
import datawave.core.common.result.ConnectionPool;
import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;
import datawave.webservice.common.connection.WrappedAccumuloClient;

/**
 * An accumulo connection factory
 */
public class AccumuloConnectionFactoryImpl implements AccumuloConnectionFactory {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final AccumuloTableCache cache;
    private final ConnectionPoolsProperties connectionPoolsConfiguration;

    private Map<String,Map<Priority,AccumuloClientPool>> pools;

    private String defaultPoolName = null;

    private static AccumuloConnectionFactoryImpl factory = null;

    public static AccumuloConnectionFactory getInstance(AccumuloTableCache cache, ConnectionPoolsProperties config) {
        if (factory == null) {
            synchronized (AccumuloConnectionFactoryImpl.class) {
                if (factory == null) {
                    setFactory(new AccumuloConnectionFactoryImpl(cache, config));
                }
            }
        }
        return factory;
    }

    private AccumuloConnectionFactoryImpl(AccumuloTableCache cache, ConnectionPoolsProperties config) {
        this.cache = cache;
        this.connectionPoolsConfiguration = config;
        log.info("Initializing AccumuloConnectionFactoryImpl with {} and {}", config.getDefaultPool(), config.getPoolNames());
        init();
    }

    public void init() {
        this.pools = new HashMap<>();

        if (this.connectionPoolsConfiguration == null) {
            log.error("connectionPoolsConfiguration was null - aborting init()");
            return;
        }
        HashMap<String,Pair<String,PasswordToken>> instances = new HashMap<>();
        this.defaultPoolName = connectionPoolsConfiguration.getDefaultPool();
        for (Entry<String,ConnectionPoolProperties> entry : connectionPoolsConfiguration.getPools().entrySet()) {
            Map<Priority,AccumuloClientPool> p = new HashMap<>();
            ConnectionPoolProperties conf = entry.getValue();
            p.put(Priority.ADMIN, createConnectionPool(conf, conf.getAdminPriorityPoolSize()));
            p.put(Priority.HIGH, createConnectionPool(conf, conf.getHighPriorityPoolSize()));
            p.put(Priority.NORMAL, createConnectionPool(conf, conf.getNormalPriorityPoolSize()));
            p.put(Priority.LOW, createConnectionPool(conf, conf.getLowPriorityPoolSize()));
            this.pools.put(entry.getKey(), Collections.unmodifiableMap(p));
            try {
                setupMockAccumuloUser(conf, p.get(AccumuloConnectionFactory.Priority.NORMAL), instances);
            } catch (Exception e) {
                log.error("Error configuring mock accumulo user for AccumuloConnectionFactoryBean.", e);
            }

            // Initialize the distributed tracing system. This needs to be done once at application startup. Since
            // it is tied to Accumulo connections, we do it here in this singleton bean.
            String appName = "datawave_ws";
            try {
                appName = System.getProperty("app", "datawave_ws");
            } catch (SecurityException e) {
                log.warn("Unable to retrieve system property \"app\": {}", e.getMessage());
            }
        }

        cache.setConnectionFactory(this);
    }

    private AccumuloClientPool createConnectionPool(ConnectionPoolProperties conf, int limit) {
        AccumuloClientPoolFactory factory = new AccumuloClientPoolFactory(conf.getUsername(), conf.getPassword(), conf.getZookeepers(), conf.getInstance());
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

    private void setupMockAccumuloUser(ConnectionPoolProperties conf, AccumuloClientPool pool, HashMap<String,Pair<String,PasswordToken>> instances)
                    throws Exception {
        AccumuloClient c = null;
        try {
            c = pool.borrowObject(new HashMap<>());

            Pair<String,PasswordToken> pair = instances.get(cache.getInstance().getInstanceID());
            String user = "root";
            PasswordToken password = new PasswordToken(new byte[0]);
            if (pair != null && user.equals(pair.getFirst()))
                password = pair.getSecond();
            SecurityOperations security = new InMemoryAccumuloClient(user, cache.getInstance()).securityOperations();
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

    private static void setFactory(AccumuloConnectionFactoryImpl factory) {
        AccumuloConnectionFactoryImpl.factory = factory;
    }

    @Override
    public void close() {
        synchronized (AccumuloConnectionFactoryImpl.class) {
            setFactory(null);
            for (Entry<String,Map<Priority,AccumuloClientPool>> entry : this.pools.entrySet()) {
                for (Entry<Priority,AccumuloClientPool> poolEntry : entry.getValue().entrySet()) {
                    try {
                        poolEntry.getValue().close();
                    } catch (Exception e) {
                        log.error("Error closing Accumulo Connection Pool: ", e);
                    }
                }
            }
        }
    }

    /**
     * Gets a connection from the pool with the assigned priority
     *
     * Deprecated in 2.2.3, use getConnection(UserContext context, String poolName, Priority priority, {@code Map<String, String> trackingMap)}
     *
     * @param priority
     *            the connection's Priority
     * @return accumulo connection
     * @throws Exception
     */
    @Override
    public AccumuloClient getClient(final String userDN, final Collection<String> proxyServers, Priority priority, Map<String,String> trackingMap)
                    throws Exception {
        return getClient(userDN, proxyServers, defaultPoolName, priority, trackingMap);
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
    @Override
    public AccumuloClient getClient(final String userDN, final Collection<String> proxyServers, final String cpn, final Priority priority,
                    final Map<String,String> tm) throws Exception {
        final Map<String,String> trackingMap = (tm != null) ? tm : new HashMap<>();
        final String poolName = (cpn != null) ? cpn : defaultPoolName;

        if (!priority.equals(Priority.ADMIN)) {
            if (userDN != null)
                trackingMap.put(USER_DN, userDN);
            if (proxyServers != null)
                trackingMap.put(PROXY_SERVERS, StringUtils.join(proxyServers, " -> "));
        }
        log.info("Getting pool from {} for priority {}", poolName, priority);
        log.info("Pools = {}", pools);
        log.info("Pools.get(poolName) = {}", pools.get(poolName));
        AccumuloClientPool pool = pools.get(poolName).get(priority);
        AccumuloClient c = pool.borrowObject(trackingMap);
        AccumuloClient mock = new InMemoryAccumuloClient(pool.getFactory().getUsername(), cache.getInstance());
        WrappedAccumuloClient wrappedAccumuloClient = new WrappedAccumuloClient(c, mock);
        if (connectionPoolsConfiguration.getClientConfiguration(poolName) != null) {
            wrappedAccumuloClient.setClientConfig(connectionPoolsConfiguration.getClientConfiguration(poolName).getConfiguration());
        }
        String classLoaderContext = System.getProperty("dw.accumulo.classLoader.context");
        if (classLoaderContext != null) {
            wrappedAccumuloClient.setScannerClassLoaderContext(classLoaderContext);
        }
        String timeout = System.getProperty("dw.accumulo.scan.batch.timeout.seconds");
        if (timeout != null) {
            wrappedAccumuloClient.setScanBatchTimeoutSeconds(Long.parseLong(timeout));
        }
        return wrappedAccumuloClient;
    }

    /**
     * Returns the connection to the pool with the associated priority.
     *
     * @param client
     *            The connection to return
     * @throws Exception
     */
    @Override
    public void returnClient(AccumuloClient client) throws Exception {
        if (client instanceof WrappedAccumuloClient) {
            WrappedAccumuloClient wrappedAccumuloClient = (WrappedAccumuloClient) client;
            wrappedAccumuloClient.clearScannerClassLoaderContext();
            client = wrappedAccumuloClient.getReal();
        }
        for (Entry<String,Map<Priority,AccumuloClientPool>> entry : this.pools.entrySet()) {
            for (Entry<Priority,AccumuloClientPool> poolEntry : entry.getValue().entrySet()) {
                if (poolEntry.getValue().connectorCameFromHere(client)) {
                    poolEntry.getValue().returnObject(client);
                    log.info("Returning connection to pool {} for priority {}", entry.getKey(), poolEntry.getKey());
                    return;
                }
            }
        }
        log.info("returnConnection called with connection that did not come from any AccumuloConnectionPool");
    }

    @Override
    public String report() {
        StringBuilder buf = new StringBuilder();
        for (Entry<String,Map<Priority,AccumuloClientPool>> entry : this.pools.entrySet()) {
            buf.append("**** ").append(entry.getKey()).append(" ****\n");
            buf.append("ADMIN: ").append(entry.getValue().get(Priority.ADMIN)).append("\n");
            buf.append("HIGH: ").append(entry.getValue().get(Priority.HIGH)).append("\n");
            buf.append("NORMAL: ").append(entry.getValue().get(Priority.NORMAL)).append("\n");
            buf.append("LOW: ").append(entry.getValue().get(Priority.LOW)).append("\n");
        }

        return buf.toString();
    }

    /**
     * Returns metrics for the AccumuloConnectionFactory
     *
     * @return list of ConnectionPool (connection pool metrics)
     */
    @Override
    public List<ConnectionPool> getConnectionPools() {
        ArrayList<ConnectionPool> connectionPools = new ArrayList<>();

        Set<String> exclude = new HashSet<>();
        exclude.add("connection.state.start");
        exclude.add("state");
        exclude.add("request.location");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

        for (Entry<String,Map<Priority,AccumuloClientPool>> entry : this.pools.entrySet()) {
            for (Entry<Priority,AccumuloClientPool> entry2 : entry.getValue().entrySet()) {
                String poolName = entry.getKey();
                Priority priority = entry2.getKey();
                AccumuloClientPool p = entry2.getValue();

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
                    for (Entry<String,String> e : m.entrySet()) {
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
        return connectionPools;
    }

    @Override
    public int getConnectionUsagePercent() {
        double maxPercentage = 0.0;
        for (Entry<String,Map<Priority,AccumuloClientPool>> entry : pools.entrySet()) {
            for (Entry<Priority,AccumuloClientPool> poolEntry : entry.getValue().entrySet()) {
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
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        HashMap<String,String> trackingMap = new HashMap<>();
        if (stackTrace != null) {
            StackTraceElement ste = stackTrace[1];
            trackingMap.put(REQUEST_LOCATION, ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber());
        }

        return trackingMap;
    }
}

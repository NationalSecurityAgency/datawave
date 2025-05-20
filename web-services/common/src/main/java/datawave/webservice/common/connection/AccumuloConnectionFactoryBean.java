package datawave.webservice.common.connection;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJB;
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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.common.result.ConnectionFactoryResponse;
import datawave.core.common.result.ConnectionPool;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;

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

    private Logger log = Logger.getLogger(this.getClass());

    @Resource
    private EJBContext context;

    @EJB
    private AccumuloTableCache cache;

    @Inject
    @ConfigProperty(name = "dw.connectionPool.default", defaultValue = "WAREHOUSE")
    private String defaultPool = null;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Inject
    @ConfigProperty(name = "dw.connectionPool.pools", defaultValue = "WAREHOUSE,METRICS")
    private List<String> poolNames;

    private AccumuloConnectionFactory factory;

    @PostConstruct
    public void init() {
        ConnectionPoolsConfiguration config = new ConnectionPoolsConfiguration().withDefaultPool(defaultPool).withPoolNames(poolNames).build();
        factory = AccumuloConnectionFactoryImpl.getInstance(cache, config);
    }

    @PreDestroy
    public void tearDown() {
        close();
    }

    @Override
    public void close() {
        try {
            factory.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            factory = null;
        }
    }

    /**
     * Gets a client from the pool with the assigned priority
     *
     * Deprecated in 2.2.3, use getClient(String poolName, Priority priority, {@code Map<String, String> trackingMap)}
     *
     * @param priority
     *            the client's Priority
     * @param trackingMap
     *            a tracking map
     * @return accumulo client
     * @throws Exception
     *             if there are issues
     */
    public AccumuloClient getClient(Priority priority, Map<String,String> trackingMap) throws Exception {
        return getClient(getCurrentUserDN(), getCurrentProxyServers(), priority, trackingMap);
    }

    @Override
    public AccumuloClient getClient(String userDN, Collection<String> proxyServers, Priority priority, Map<String,String> trackingMap) throws Exception {
        return factory.getClient(userDN, proxyServers, priority, trackingMap);
    }

    /**
     * Gets a client from the named pool with the assigned priority
     *
     * @param cpn
     *            the name of the pool to retrieve the client from
     * @param priority
     *            the priority of the client
     * @param trackingMap
     *            the tracking map
     * @return Accumulo client
     * @throws Exception
     *             if there are issues
     */
    public AccumuloClient getClient(final String cpn, final Priority priority, final Map<String,String> trackingMap) throws Exception {
        return getClient(getCurrentUserDN(), getCurrentProxyServers(), cpn, priority, trackingMap);
    }

    @Override
    public AccumuloClient getClient(String userDN, Collection<String> proxyServers, String cpn, Priority priority, Map<String,String> trackingMap)
                    throws Exception {
        return factory.getClient(userDN, proxyServers, cpn, priority, trackingMap);
    }

    /**
     * Returns the client to the pool with the associated priority.
     *
     * @param client
     *            The client to return
     * @throws Exception
     *             if there are issues
     */
    @PermitAll
    // permit anyone to return a connection
    public void returnClient(AccumuloClient client) throws Exception {
        factory.returnClient(client);
    }

    @PermitAll
    // permit anyone to get the report
    @JmxManaged
    public String report() {
        return factory.report();
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
        response.setConnectionPools(getConnectionPools());
        return response;
    }

    @Override
    public List<ConnectionPool> getConnectionPools() {
        return factory.getConnectionPools();
    }

    @PermitAll
    @JmxManaged
    public int getConnectionUsagePercent() {
        return factory.getConnectionUsagePercent();
    }

    @Override
    @PermitAll
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        return factory.getTrackingMap(stackTrace);
    }

    public String getCurrentUserDN() {

        String currentUserDN = null;
        Principal p = context.getCallerPrincipal();

        if (p instanceof DatawavePrincipal) {
            currentUserDN = ((DatawavePrincipal) p).getUserDN().subjectDN();
        }

        return currentUserDN;
    }

    public Collection<String> getCurrentProxyServers() {
        List<String> currentProxyServers = null;
        Principal p = context.getCallerPrincipal();

        if (p instanceof DatawavePrincipal) {
            currentProxyServers = ((DatawavePrincipal) p).getProxyServers();
        }

        return currentProxyServers;
    }
}

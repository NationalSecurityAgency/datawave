package datawave.webservice.common.cache;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Local;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.annotation.Required;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.cache.AccumuloTableCache;
import datawave.core.common.cache.AccumuloTableCacheImpl;
import datawave.core.common.cache.AccumuloTableCacheProperties;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.AccumuloTableCacheStatus;
import datawave.core.common.result.TableCacheDescription;
import datawave.interceptor.RequiredInterceptor;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;

/**
 * Object that caches data from Accumulo tables.
 */
@Path("/Common/AccumuloTableCache")
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Local(AccumuloTableCache.class)
@LocalBean
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.READ)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class AccumuloTableCacheBean implements AccumuloTableCache {

    private final Logger log = Logger.getLogger(this.getClass());

    @Inject
    private JMSContext jmsContext;

    @Resource(mappedName = "java:/topic/AccumuloTableCache")
    private Destination cacheTopic;

    @Resource
    private ManagedExecutorService executorService;

    @Inject
    @ConfigProperty(name = "dw.warehouse.zookeepers")
    private String zookeepers = null;
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Inject
    @ConfigProperty(name = "dw.cache.tableNames", defaultValue = "DatawaveMetadata,QueryMetrics_m,errorMetadata")
    private List<String> tableNames;
    @Inject
    @ConfigProperty(name = "dw.cache.pool", defaultValue = "WAREHOUSE")
    private String poolName;
    @Inject
    @ConfigProperty(name = "dw.cache.reloadInterval", defaultValue = "86400000")
    private long reloadInterval;
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.evictionReaperIntervalSeconds", defaultValue = "30")
    private int evictionReaperIntervalInSeconds;
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.numLocks", defaultValue = "300")
    private int numLocks;
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.maxRetries", defaultValue = "10")
    private int maxRetries;

    private AccumuloTableCacheImpl tableCache;

    public AccumuloTableCacheBean() {}

    @PostConstruct
    private void setup() {
        AccumuloTableCacheProperties config = new AccumuloTableCacheProperties().withTableNames(tableNames).withPoolName(poolName).withNumLocks(numLocks)
                        .withZookeepers(zookeepers).withMaxRetries(maxRetries).withReloadInterval(reloadInterval)
                        .withEvictionReaperIntervalInSeconds(evictionReaperIntervalInSeconds);

        log.debug("Called AccumuloTableCacheBean and accumuloTableCacheConfiguration = " + config);

        tableCache = new AccumuloTableCacheImpl(executorService, config);
    }

    @Override
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory) {
        tableCache.setConnectionFactory(connectionFactory);
    }

    @Override
    public InMemoryInstance getInstance() {
        return tableCache.getInstance();
    }

    @Schedule(hour = "*", minute = "*", second = "1", persistent = false)
    @Override
    public void submitReloadTasks() {
        tableCache.submitReloadTasks();
    }

    @PreDestroy
    public void stop() {
        close();
    }

    @Override
    public void close() {
        tableCache.close();
        tableCache = null;
    }

    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong>
     *
     * @param tableName
     *            the name of the table for which the cached version is to be reloaded
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 queries not found using {@code id}
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/reload/{tableName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors(RequiredInterceptor.class)
    public VoidResponse reloadCache(@Required("tableName") @PathParam("tableName") String tableName) {
        VoidResponse response = new VoidResponse();
        try {
            reloadTableCache(tableName);
        } catch (Exception e) {
            response.addException(new QueryException(e).getBottomQueryException());
            throw new DatawaveWebApplicationException(e, response);
        }
        return response;
    }

    @Override
    public void reloadTableCache(String tableName) {
        tableCache.reloadTableCache(tableName);
        sendCacheReloadMessage(tableName);
    }

    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong>
     *
     * @return datawave.webservice.common.result.AccumuloTableCacheStatus
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     */
    @GET
    @Path("/")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff", "text/html"})
    @GZIP
    public AccumuloTableCacheStatus getStatus() {
        AccumuloTableCacheStatus response = new AccumuloTableCacheStatus();
        response.getCaches().addAll(getTableCaches());
        return response;
    }

    @Override
    public List<TableCacheDescription> getTableCaches() {
        return tableCache.getTableCaches();
    }

    private void sendCacheReloadMessage(String tableName) {
        log.warn("table:" + tableName + " sending cache reload message about table " + tableName);

        jmsContext.createProducer().send(cacheTopic, tableName);
    }

}

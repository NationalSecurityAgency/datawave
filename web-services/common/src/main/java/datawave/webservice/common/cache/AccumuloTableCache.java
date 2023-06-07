package datawave.webservice.common.cache;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBException;
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

import datawave.annotation.Required;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.interceptor.RequiredInterceptor;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.result.AccumuloTableCacheStatus;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

/**
 * Object that caches data from Accumulo tables.
 */
@Path("/Common/AccumuloTableCache")
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
@LocalBean
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.READ)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class AccumuloTableCache {
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private JMSContext jmsContext;
    
    @Resource(mappedName = "java:/topic/AccumuloTableCache")
    private Destination cacheTopic;
    
    @Resource
    private ManagedExecutorService executorService;
    
    @Inject
    private AccumuloTableCacheConfiguration accumuloTableCacheConfiguration;
    
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.evictionReaperIntervalSeconds", defaultValue = "30")
    private int evictionReaperIntervalInSeconds;
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.numLocks", defaultValue = "300")
    private int numLocks;
    @Inject
    @ConfigProperty(name = "dw.cacheCoordinator.maxRetries", defaultValue = "10")
    private int maxRetries;
    
    public static final String MOCK_USERNAME = "";
    public static final PasswordToken MOCK_PASSWORD = new PasswordToken(new byte[0]);
    
    private InMemoryInstance instance;
    private Map<String,TableCache> details;
    private List<SharedCacheCoordinator> cacheCoordinators;
    private boolean connectionFactoryProvided = false;
    
    public AccumuloTableCache() {
        log.debug("Called AccumuloTableCacheBean and accumuloTableCacheConfiguration = " + accumuloTableCacheConfiguration);
    }
    
    @PostConstruct
    private void setup() {
        log.debug("accumuloTableCacheConfiguration was setup as: " + accumuloTableCacheConfiguration);
        
        instance = new InMemoryInstance();
        details = new HashMap<>();
        cacheCoordinators = new ArrayList<>();
        
        String zookeepers = accumuloTableCacheConfiguration.getZookeepers();
        
        for (Entry<String,TableCache> entry : accumuloTableCacheConfiguration.getCaches().entrySet()) {
            final String tableName = entry.getKey();
            TableCache detail = entry.getValue();
            detail.setInstance(instance);
            
            final SharedCacheCoordinator cacheCoordinator = new SharedCacheCoordinator(tableName, zookeepers, evictionReaperIntervalInSeconds, numLocks,
                            maxRetries);
            cacheCoordinators.add(cacheCoordinator);
            try {
                cacheCoordinator.start();
            } catch (Exception e) {
                throw new EJBException("Error starting AccumuloTableCache", e);
            }
            
            try {
                cacheCoordinator.registerTriState(tableName + ":needsUpdate", new SharedTriStateListener() {
                    @Override
                    public void stateHasChanged(SharedTriStateReader reader, SharedTriState.STATE value) throws Exception {
                        if (log.isTraceEnabled()) {
                            log.trace("table:" + tableName + " stateHasChanged(" + reader + ", " + value + "). This listener does nothing");
                        }
                    }
                    
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        if (log.isTraceEnabled()) {
                            log.trace("table:" + tableName + " stateChanged(" + client + ", " + newState + "). This listener does nothing");
                        }
                    }
                });
            } catch (Exception e) {
                log.debug("Failure registering a triState for " + tableName, e);
            }
            
            try {
                cacheCoordinator.registerCounter(tableName, new SharedCountListener() {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        // TODO Auto-generated method stub
                    }
                    
                    @Override
                    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                        if (!cacheCoordinator.checkCounter(tableName, newCount)) {
                            handleReload(tableName);
                        }
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException("table:" + tableName + " Unable to create shared counters: " + e.getMessage(), e);
            }
            detail.setWatcher(cacheCoordinator);
            details.put(entry.getKey(), entry.getValue());
            
        }
    }
    
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory) {
        for (Entry<String,TableCache> entry : accumuloTableCacheConfiguration.getCaches().entrySet()) {
            TableCache detail = entry.getValue();
            detail.setConnectionFactory(connectionFactory);
        }
        connectionFactoryProvided = true;
    }
    
    public InMemoryInstance getInstance() {
        return this.instance;
    }
    
    @Schedule(hour = "*", minute = "*", second = "1", persistent = false)
    public void submitReloadTasks() {
        if (!connectionFactoryProvided) {
            log.trace("NOT submitting reload tasks since our connection factory hasn't been provided yet.");
            return;
        }
        
        // Check results of running tasks. If complete, set the references to null
        for (Entry<String,TableCache> entry : details.entrySet()) {
            Future<Boolean> ref = entry.getValue().getReference();
            if (null != ref && (ref.isCancelled() || ref.isDone())) {
                log.info("Reloading complete for table: " + entry.getKey());
                entry.getValue().setReference(null);
            }
            
        }
        // Start new tasks
        long now = System.currentTimeMillis();
        for (Entry<String,TableCache> entry : details.entrySet()) {
            if (null != entry.getValue().getReference()) {
                continue;
            }
            long last = entry.getValue().getLastRefresh().getTime();
            if ((now - last) > entry.getValue().getReloadInterval()) {
                log.info("Reloading " + entry.getKey());
                try {
                    Future<Boolean> result = executorService.submit(entry.getValue());
                    entry.getValue().setReference(result);
                } catch (Exception e) {
                    log.error("Error reloading table: " + entry.getKey(), e);
                }
            }
        }
    }
    
    @PreDestroy
    public void stop() {
        for (Entry<String,TableCache> entry : details.entrySet()) {
            Future<Boolean> ref = entry.getValue().getReference();
            if (null != ref)
                ref.cancel(true);
        }
        for (SharedCacheCoordinator cacheCoordinator : cacheCoordinators)
            cacheCoordinator.stop();
        cacheCoordinators.clear();
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
        if (null == details.get(tableName)) {
            return response;
        }
        // send an eviction notice to the cluster
        try {
            details.get(tableName).getWatcher().incrementCounter(tableName);
        } catch (Exception e) {
            response.addException(new QueryException(e).getBottomQueryException());
            throw new DatawaveWebApplicationException(e, response);
        }
        try {
            this.sendCacheReloadMessage(tableName);
        } catch (Exception e) {
            log.error("Unable to send message about cache reload");
        }
        handleReload(tableName);
        handleReloadTypeMetadata(tableName);
        return response;
    }
    
    private void handleReloadTypeMetadata(String tableName) {
        String triStateName = tableName + ":needsUpdate";
        try {
            log.debug(triStateName + " handleReloadTypeMetadata(" + tableName + ")");
            SharedCacheCoordinator watcher = details.get(tableName).getWatcher();
            if (!watcher.checkTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE)) {
                watcher.setTriState(triStateName, SharedTriState.STATE.NEEDS_UPDATE);
            }
        } catch (Throwable e) {
            log.debug("table:" + tableName + " could not update the triState '" + triStateName + " on watcher for table " + tableName, e);
        }
    }
    
    private void handleReload(String tableName) {
        details.get(tableName).setLastRefresh(new Date(0));
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
        for (Entry<String,TableCache> entry : details.entrySet()) {
            datawave.webservice.common.result.TableCache t = new datawave.webservice.common.result.TableCache();
            t.setTableName(entry.getValue().getTableName());
            t.setConnectionPoolName(entry.getValue().getConnectionPoolName());
            t.setAuthorizations(entry.getValue().getAuths());
            t.setReloadInterval(entry.getValue().getReloadInterval());
            t.setMaxRows(entry.getValue().getMaxRows());
            t.setLastRefresh(entry.getValue().getLastRefresh());
            t.setCurrentlyRefreshing((entry.getValue().getReference() != null));
            response.getCaches().add(t);
        }
        return response;
    }
    
    private void sendCacheReloadMessage(String tableName) {
        log.warn("table:" + tableName + " sending cache reload message about table " + tableName);
        
        jmsContext.createProducer().send(cacheTopic, tableName);
    }
}

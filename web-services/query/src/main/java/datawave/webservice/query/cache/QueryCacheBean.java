package datawave.webservice.query.cache;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.util.Pair;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Map.Entry;

@Path("/Query/Cache")
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.READ)
// by default all methods are non-blocking
@MBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryCacheBean {
    
    @Inject
    private QueryCache cache;
    
    @Inject
    private QueryExecutorBean query;
    
    @Inject
    private CreatedQueryLogicCacheBean qlCache;
    
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @JmxManaged
    public String listRunningQueries() {
        RunningQueries rq = getRunningQueries();
        StringBuilder buf = new StringBuilder();
        for (String query : rq.getQueries()) {
            buf.append(query).append("\n");
        }
        return buf.toString();
    }
    
    /**
     * <strong>Administrator credentials required.</strong>
     *
     * @return running queries
     */
    @GET
    @Path("/listRunningQueries")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    public RunningQueries getRunningQueries() {
        
        RunningQueries result = new RunningQueries();
        
        // Iterate over the cache contents
        for (RunningQuery value : cache) {
            result.getQueries().add(value.toString());
        }
        // Iterate over queries that are in the init phase
        for (Entry<String,Pair<QueryLogic<?>,AccumuloClient>> entry : qlCache.snapshot().entrySet()) {
            result.getQueries().add("Identifier: " + entry.getKey() + " Query Logic: " + entry.getValue().getFirst().getClass().getName() + "\n");
        }
        return result;
    }
    
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @JmxManaged
    public String cancelUserQuery(String id) throws Exception {
        AbstractRunningQuery arq = cache.get(id);
        if (arq != null) {
            try {
                @SuppressWarnings("unused")
                VoidResponse response = query.cancel(id);
                return "Success.";
            } catch (Exception e) {
                return "Failed, check log.";
            }
        } else {
            return "No such query: " + id;
        }
    }
    
}

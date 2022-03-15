package datawave.webservice.query.hud;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.query.Query;
import datawave.webservice.query.factory.Persister;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetricSummary;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.result.QueryImplListResponse;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.resteasy.annotations.GZIP;

@Path("/Query/queryhud")
@GZIP
@Produces(MediaType.APPLICATION_JSON)
@RolesAllowed("AuthorizedUser")
@DeclareRoles("AuthorizedUser")
@Stateless
@LocalBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class HudBean {
    
    private Gson gson = new Gson();
    private HudQuerySummaryBuilder summaryBuilder = new HudQuerySummaryBuilder();
    private HudMetricSummaryBuilder metricSummaryBuilder = new HudMetricSummaryBuilder();
    
    @Inject
    private QueryExecutorBean queryExecutor;
    @Inject
    private QueryMetricsBean queryMetrics;
    @Inject
    private Persister persister;
    
    @Resource
    protected EJBContext ctx;
    
    /**
     * Return true if there is at least one log in the last 60 minutes.
     *
     * @return
     * @throws org.apache.accumulo.core.client.TableNotFoundException
     */
    @Path("/runningqueries/{userid}")
    @GET
    public String getRunningQueries(@PathParam("userid") String userId) throws Exception {
        DatawavePrincipal principal = getPrincipal();
        boolean isAnAdmin = isAnAdmin(principal);
        QueryImplListResponse runningQueries = null;
        if (isAnAdmin) {
            runningQueries = queryExecutor.listQueriesForUser(userId);
        } else {
            runningQueries = queryExecutor.listUserQueries();
        }
        
        List<Query> queryList = runningQueries.getQuery();
        List<HudQuerySummary> querySummaryList = new ArrayList<>();
        
        for (Query query : queryList) {
            HudQuerySummary summary = summaryBuilder.build(query);
            
            String queryId = query.getId().toString();
            
            List<? extends BaseQueryMetric> queryMetricsList;
            
            queryMetricsList = queryMetrics.query(queryId).getResult();
            
            if (queryMetricsList != null && !queryMetricsList.isEmpty()) {
                BaseQueryMetric qm = queryMetricsList.get(0);
                
                List<PageMetric> pageMetrics = qm.getPageTimes();
                summary.setPageMetrics(pageMetrics);
                
                summary.setCreateDate(qm.getCreateDate().getTime());
                summary.setNumPages(qm.getNumPages());
                summary.setNumResults(qm.getNumResults());
                summary.setLastUpdated(qm.getLastUpdated().getTime());
                summary.setLifeCycle(qm.getLifecycle().toString());
            }
            
            querySummaryList.add(summary);
        }
        
        return gson.toJson(querySummaryList);
    }
    
    private DatawavePrincipal getPrincipal() {
        Principal p = ctx.getCallerPrincipal();
        
        if (p instanceof DatawavePrincipal) {
            return (DatawavePrincipal) p;
        }
        
        throw new IllegalArgumentException("Principal must be of the correct type");
    }
    
    @Path("/summaryall")
    @GET
    @RolesAllowed({"Administrator", "MetricsAdministrator"})
    public String getSummaryQueryStats() throws Exception {
        QueryMetricsSummaryResponse summaryResp = queryMetrics.getQueryMetricsSummary(null, null);
        QueryMetricSummary hour1 = summaryResp.getHour1();
        QueryMetricSummary hour6 = summaryResp.getHour6();
        QueryMetricSummary hour12 = summaryResp.getHour12();
        QueryMetricSummary day1 = summaryResp.getDay1();
        
        List<HudMetricSummary> metricSummaryList = new ArrayList<>();
        metricSummaryList.add(metricSummaryBuilder.buildMetricsSummary(1L, hour1));
        metricSummaryList.add(metricSummaryBuilder.buildMetricsSummary(6L, hour6));
        metricSummaryList.add(metricSummaryBuilder.buildMetricsSummary(12L, hour12));
        metricSummaryList.add(metricSummaryBuilder.buildMetricsSummary(24L, day1));
        
        return gson.toJson(metricSummaryList);
    }
    
    @Path("/activeusers")
    @GET
    public String getActiveUsers() throws Exception {
        return gson.toJson(new HashSet<>());
        
        // 08/10/2016: Found this while cleaning up commented code. Should this entire method be removed, since it currently does nothing? Or fixed?
        
        // I removed the logic below as it was calling a method on the persister bean
        // that did not do what it was intended to do.
        /*
         * Set<HudActiveUser> hudActiveUsers = new HashSet<>(); boolean isAnAdmin = false; Principal p = ctx.getCallerPrincipal(); if (p instanceof
         * DatawavePrincipal) { DatawavePrincipal dp = (DatawavePrincipal)p; hudActiveUsers.add(new HudActiveUser(dp.getSid())); isAnAdmin = isAnAdmin(dp); }
         * 
         * try { // If they are an admin, make a call to the Persister EJB to get the list of // active users, otherwise they can only query for themselves.
         * if(isAnAdmin){ List<String> activeUsers = persister.findActiveUsers(); for(String sid : activeUsers) { hudActiveUsers.add(new HudActiveUser(sid)); }
         * }
         * 
         * } catch (Exception e) { throw new EJBException("Error getting the list of active users running queries", e); } String activeUsersJson = null;
         * activeUsersJson = gson.toJson(hudActiveUsers.toArray()); return activeUsersJson;
         */
    }
    
    private boolean isAnAdmin(DatawavePrincipal dp) {
        Collection<String> roles = dp.getPrimaryUser().getRoles();
        return roles.contains("Administrator") || roles.contains("JBossAdministrator");
    }
}

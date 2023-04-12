package datawave.webservice.query.dashboard;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.AuthorizationsUtil;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.extjs.ExtJsResponse;
import datawave.webservice.query.runner.QueryExecutorBean;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.security.Principal;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Path("/Query/Metrics/dashboard")
@GZIP
@Produces(MediaType.APPLICATION_JSON)
@Stateless
@LocalBean
@RolesAllowed("AuthorizedUser")
@DeclareRoles("AuthorizedUser")
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DashboardBean {
    
    private static final Logger log = Logger.getLogger(DashboardBean.class);
    private static final long MS_IN_12_HRS = 43_200_000;// timestamps seem to be about 11 hours behind
    private static final String TABLE_NAME_JMC = "DpsJmcLogs";
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    @Inject
    private QueryExecutorBean queryExecutor;
    @Resource
    protected EJBContext ctx;
    
    @Path("/dpsjmc/heartbeat")
    @GET
    public boolean getHeartbeat() throws Exception {
        Connector c = null;
        try {
            c = createConnector();
            return createScanner(c).iterator().hasNext();
        } finally {
            try {
                connectionFactory.returnConnection(c);
            } catch (Exception e) {
                log.error("Error returning connection to factory.", e);
            }
        }
    }
    
    @GET
    @Path("/stats")
    @Interceptors(ResponseInterceptor.class)
    public ExtJsResponse<DashboardSummary> getQuerySummary(@QueryParam("start") long startMs, @QueryParam("end") long endMs) throws Exception {
        Instant now = Instant.now();
        Instant start = Instant.ofEpochMilli(startMs);
        Instant end = Instant.ofEpochMilli(endMs);
        String auths;
        DatawavePrincipal principal = getPrincipal();
        if (principal == null) {
            auths = "ALL";
        } else {
            auths = AuthorizationsUtil.buildAuthorizationString(principal.getAuthorizations());
        }
        
        ExtJsResponse<DashboardSummary> summary = null;
        try {
            summary = DashboardQuery.createQuery(queryExecutor, auths, Date.from(start), Date.from(end), Date.from(now));
        } catch (RuntimeException ex) {
            log.error("An error occurred querying for dashboard metrics: " + ex.getMessage(), ex);
            throw ex;
        } finally {
            if (summary != null) {
                queryExecutor.close(summary.getQueryId());
            }
        }
        
        return summary;
    }
    
    private DatawavePrincipal getPrincipal() {
        Principal p = ctx.getCallerPrincipal();
        
        if (p instanceof DatawavePrincipal) {
            return (DatawavePrincipal) p;
        }
        
        log.warn("Principal is not of the correct type");
        
        return null;
    }
    
    private Set<Authorizations> getAuths() {
        DatawavePrincipal dp = getPrincipal();
        Set<Authorizations> auths = new HashSet<>();
        
        for (Collection<String> cbAuths : dp.getAuthorizations()) {
            auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        
        return auths;
    }
    
    /**
     * Create scanner for last 60 minutes of logs.
     *
     * @param c
     *            the {@link Connector} to use when creating scanners
     *
     * @return a {@link Scanner} that will only scan over the last 60 minutes of logs
     *
     * @throws TableNotFoundException
     *             if the table is not found
     */
    private Scanner createScanner(Connector c) throws TableNotFoundException {
        long start = Instant.now().toEpochMilli() - MS_IN_12_HRS;
        long end = start + (1000 * 60 * 10);// 10 minutes
        Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME_JMC, getAuths());
        Key startKey = new Key(Long.toString(start));
        Key endKey = new Key(Long.toString(end));
        Range range = new Range(startKey, endKey);
        scanner.setRange(range);
        return scanner;
    }
    
    private Connector createConnector() throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        return connectionFactory.getConnection(AccumuloConnectionFactory.Priority.LOW, trackingMap);
    }
}

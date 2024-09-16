package datawave.webservice.query.dashboard;

import java.security.Principal;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.extjs.ExtJsResponse;
import datawave.core.query.dashboard.DashboardSummary;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.query.runner.QueryExecutorBean;

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
        AccumuloClient c = null;
        try {
            c = createClient();
            return createScanner(c).iterator().hasNext();
        } finally {
            try {
                connectionFactory.returnClient(c);
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
            auths = WSAuthorizationsUtil.buildAuthorizationString(principal.getAuthorizations());
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
     * @param accumuloClient
     *            the {@link AccumuloClient} to use when creating scanners
     *
     * @return a {@link Scanner} that will only scan over the last 60 minutes of logs
     *
     * @throws TableNotFoundException
     *             if the table is not found
     */
    private Scanner createScanner(AccumuloClient accumuloClient) throws TableNotFoundException {
        long start = Instant.now().toEpochMilli() - MS_IN_12_HRS;
        long end = start + (1000 * 60 * 10);// 10 minutes
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, TABLE_NAME_JMC, getAuths());
        Key startKey = new Key(Long.toString(start));
        Key endKey = new Key(Long.toString(end));
        Range range = new Range(startKey, endKey);
        scanner.setRange(range);
        return scanner;
    }

    private AccumuloClient createClient() throws Exception {
        Principal p = ctx.getCallerPrincipal();
        String userDn = null;
        Collection<String> proxyServers = null;
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userDn = dp.getUserDN().subjectDN();
            proxyServers = dp.getProxyServers();
        }
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        return connectionFactory.getClient(userDn, proxyServers, AccumuloConnectionFactory.Priority.LOW, trackingMap);
    }
}

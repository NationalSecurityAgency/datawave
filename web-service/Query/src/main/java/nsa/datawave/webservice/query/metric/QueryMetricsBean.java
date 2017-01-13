package nsa.datawave.webservice.query.metric;

import java.security.Principal;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import nsa.datawave.annotation.DateFormat;
import nsa.datawave.annotation.Required;
import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.interceptor.RequiredInterceptor;
import nsa.datawave.interceptor.ResponseInterceptor;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.PageMetric;

import org.apache.commons.lang.time.DateUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

@Path("/Query/Metrics")
@Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
@GZIP
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "MetricsAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "MetricsAdministrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryMetricsBean {
    
    private static final Logger log = Logger.getLogger(QueryMetricsBean.class);
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    @Inject
    private JMSContext jmsContext;
    @Resource(mappedName = "java:/queue/QueryMetrics")
    private Destination dest;
    @Resource
    private EJBContext ctx;
    @Inject
    private QueryMetricHandler<? extends BaseQueryMetric> queryHandler;
    
    public QueryMetricsBean() {}
    
    public void updateMetric(BaseQueryMetric metric) throws Exception {
        DatawavePrincipal dp = getPrincipal();
        
        if (metric.getLastWrittenHash() != metric.hashCode()) {
            metric.setLastWrittenHash(metric.hashCode());
            try {
                metric.setLastUpdated(new Date());
                sendQueryMetric(dp, metric);
                // PageMetrics now know their own page numbers
                // this should keep large queries from blowing up the queue
                // Leave the last page on the list so that interceptors can update it.
                Iterator<PageMetric> itr = metric.getPageTimes().iterator();
                while (metric.getPageTimes().size() > 1) {
                    itr.next();
                    itr.remove();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param id
     *
     * @return nsa.datawave.webservice.result.QueryMetricListResponse
     *
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @POST
    @Path("/id/{id}")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public BaseQueryMetricListResponse query(@PathParam("id") @Required("id") String id) {
        
        // Find out who/what called this method
        DatawavePrincipal dp = null;
        Principal p = ctx.getCallerPrincipal();
        String user = p.getName();
        if (p instanceof DatawavePrincipal) {
            dp = (DatawavePrincipal) p;
            user = dp.getShortName();
        }
        return queryHandler.query(user, id, dp);
    }
    
    /**
     *
     * Returns a summary of the query metrics
     *
     * @param begin
     *            (optional)
     * @param end
     *            (optional)
     *
     * @return nsa.datawave.webservice.result.QueryMetricsSummaryResponse
     *
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/summaryCounts")
    @Interceptors(ResponseInterceptor.class)
    public QueryMetricsSummaryHtmlResponse getTotalQueriesSummary(@QueryParam("begin") @DateFormat(defaultTime = "000000", defaultMillisec = "000") Date begin,
                    @QueryParam("end") @DateFormat(defaultTime = "235959", defaultMillisec = "999") Date end) {
        
        if (null == begin) {
            // midnight of the current day
            begin = DateUtils.truncate(Calendar.getInstance(DateUtils.UTC_TIME_ZONE), Calendar.DATE).getTime();
        } else {
            begin = DateUtils.truncate(begin, Calendar.SECOND);
        }
        if (null == end) {
            end = new Date();
        } else {
            end = DateUtils.truncate(end, Calendar.SECOND);
        }
        DatawavePrincipal dp = getPrincipal();
        return queryHandler.getTotalQueriesSummary(begin, end, dp);
    }
    
    /**
     *
     * Returns a summary of the query metrics
     *
     * @param begin
     *            (optional)
     * @param end
     *            (optional)
     *
     * @return nsa.datawave.webservice.result.QueryMetricsSummaryResponse
     *
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/summary")
    @Interceptors(ResponseInterceptor.class)
    public QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(
                    @QueryParam("begin") @DateFormat(defaultTime = "000000", defaultMillisec = "000") Date begin, @QueryParam("end") @DateFormat(
                                    defaultTime = "235959", defaultMillisec = "999") Date end) {
        
        if (null == begin) {
            // midnight of ninety days ago
            Calendar ninetyDaysAgo = Calendar.getInstance(DateUtils.UTC_TIME_ZONE);
            ninetyDaysAgo.add(Calendar.DATE, -90);
            begin = DateUtils.truncate(ninetyDaysAgo, Calendar.DATE).getTime();
        } else {
            begin = DateUtils.truncate(begin, Calendar.SECOND);
        }
        if (null == end) {
            end = new Date();
        } else {
            end = DateUtils.truncate(end, Calendar.SECOND);
        }
        DatawavePrincipal dp = getPrincipal();
        return queryHandler.getTotalQueriesSummaryCounts(begin, end, dp);
    }
    
    /**
     *
     * Returns a summary of the requesting user's query metrics
     *
     * @param begin
     *            (optional)
     * @param end
     *            (optional)
     *
     * @return nsa.datawave.webservice.result.QueryMetricsSummaryResponse
     *
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/summaryCounts/user")
    @Interceptors(ResponseInterceptor.class)
    public QueryMetricsSummaryHtmlResponse getUserQueriesSummary(@QueryParam("begin") @DateFormat(defaultTime = "000000", defaultMillisec = "000") Date begin,
                    @QueryParam("end") @DateFormat(defaultTime = "235959", defaultMillisec = "999") Date end) {
        
        if (null == begin) {
            // midnight of ninety days ago
            Calendar ninetyDaysAgo = Calendar.getInstance(DateUtils.UTC_TIME_ZONE);
            ninetyDaysAgo.add(Calendar.DATE, -90);
            begin = DateUtils.truncate(ninetyDaysAgo, Calendar.DATE).getTime();
        } else {
            begin = DateUtils.truncate(begin, Calendar.SECOND);
        }
        if (null == end) {
            end = new Date();
        } else {
            end = DateUtils.truncate(end, Calendar.SECOND);
        }
        DatawavePrincipal dp = getPrincipal();
        return queryHandler.getUserQueriesSummary(begin, end, dp);
    }
    
    /**
     * Find out who/what called this method
     *
     * @return
     */
    private DatawavePrincipal getPrincipal() {
        DatawavePrincipal dp = null;
        Principal p = ctx.getCallerPrincipal();
        if (p instanceof DatawavePrincipal) {
            dp = (DatawavePrincipal) p;
        }
        return dp;
    }
    
    public void sendQueryMetric(DatawavePrincipal principal, BaseQueryMetric queryMetric) throws Exception {
        
        QueryMetricHolder queryMetricHolder = new QueryMetricHolder(principal, queryMetric);
        QueryMetricMessage msg = new QueryMetricMessage(queryMetricHolder);
        
        jmsContext.createProducer().send(dest, msg);
    }
}

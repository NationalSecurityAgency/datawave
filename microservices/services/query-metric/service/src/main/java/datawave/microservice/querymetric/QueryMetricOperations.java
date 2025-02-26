package datawave.microservice.querymetric;

import static datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import static datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.BEGIN;
import static datawave.microservice.querymetric.QueryMetricOperations.DEFAULT_DATETIME.END;
import static datawave.microservice.querymetric.QueryMetricOperationsStats.TIMERS;
import static datawave.microservice.querymetric.config.HazelcastMetricCacheConfiguration.INCOMING_METRICS;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;
import javax.annotation.security.PermitAll;
import javax.inject.Named;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.codahale.metrics.Timer;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;
import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;
import datawave.microservice.querymetric.handler.QueryGeometryHandler;
import datawave.microservice.querymetric.handler.ShardTableQueryMetricHandler;
import datawave.microservice.querymetric.handler.SimpleQueryGeometryHandler;
import datawave.microservice.security.util.DnUtils;
import datawave.query.jexl.visitors.JexlFormattedStringBuildingVisitor;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * The type Query metric operations.
 */
@Tag(name = "Query Metric Operations /v1",
                externalDocs = @ExternalDocumentation(description = "Query Metric Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-query-metric-service"))
@EnableScheduling
@RestController
@RequestMapping(path = "/v1")
public class QueryMetricOperations {
    private Logger log = LoggerFactory.getLogger(QueryMetricOperations.class);
    
    private ShardTableQueryMetricHandler handler;
    private QueryGeometryHandler geometryHandler;
    private CacheManager cacheManager;
    private Cache incomingQueryMetricsCache;
    private MarkingFunctions markingFunctions;
    private QueryMetricResponseFactory queryMetricResponseFactory;
    private MergeLockLifecycleListener mergeLock;
    private Correlator correlator;
    private AtomicBoolean timedCorrelationInProgress = new AtomicBoolean(false);
    private MetricUpdateEntryProcessorFactory entryProcessorFactory;
    private QueryMetricOperationsStats stats;
    private static Set<String> inProcess = Collections.synchronizedSet(new HashSet<>());
    
    private final QueryMetricClient queryMetricClient;
    private final DnUtils dnUtils;
    
    /**
     * The enum Default datetime.
     */
    enum DEFAULT_DATETIME {
        /**
         * Begin default datetime.
         */
        BEGIN,
        /**
         * End default datetime.
         */
        END
    }
    
    /**
     * Instantiates a new QueryMetricOperations.
     *
     * @param cacheManager
     *            the CacheManager
     * @param handler
     *            the QueryMetricHandler
     * @param geometryHandler
     *            the QueryGeometryHandler
     * @param markingFunctions
     *            the MarkingFunctions
     * @param queryMetricResponseFactory
     *            the QueryMetricResponseFactory
     * @param mergeLock
     *            the merge lock
     * @param correlator
     *            the correlator
     * @param entryProcessorFactory
     *            the entry processor factory
     * @param stats
     *            the stats
     * @param queryMetricClient
     *            the QueryMetricClient
     * @param dnUtils
     *            the dnUtils
     */
    @Autowired
    public QueryMetricOperations(@Named("queryMetricCacheManager") CacheManager cacheManager, ShardTableQueryMetricHandler handler,
                    QueryGeometryHandler geometryHandler, MarkingFunctions markingFunctions, QueryMetricResponseFactory queryMetricResponseFactory,
                    MergeLockLifecycleListener mergeLock, Correlator correlator, MetricUpdateEntryProcessorFactory entryProcessorFactory,
                    QueryMetricOperationsStats stats, QueryMetricClient queryMetricClient, DnUtils dnUtils) {
        this.handler = handler;
        this.geometryHandler = geometryHandler;
        this.cacheManager = cacheManager;
        this.incomingQueryMetricsCache = cacheManager.getCache(INCOMING_METRICS);
        this.markingFunctions = markingFunctions;
        this.queryMetricResponseFactory = queryMetricResponseFactory;
        this.mergeLock = mergeLock;
        this.correlator = correlator;
        this.entryProcessorFactory = entryProcessorFactory;
        this.stats = stats;
        this.queryMetricClient = queryMetricClient;
        this.dnUtils = dnUtils;
    }
    
    @PreDestroy
    public void shutdown() {
        if (this.correlator.isEnabled()) {
            this.correlator.shutdown(true);
            // we've locked out the timer thread, but need to
            // wait for it to complete the last write
            while (isTimedCorrelationInProgress()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    
                }
            }
            ensureUpdatesProcessed(false);
        }
        this.stats.queueAggregatedQueryStatsForTimely();
        this.stats.writeQueryStatsToTimely();
    }
    
    public boolean isTimedCorrelationInProgress() {
        return this.timedCorrelationInProgress.get();
    }
    
    @Scheduled(fixedDelay = 2000)
    public void ensureUpdatesProcessedScheduled() {
        // don't write metrics from this thread while shutting down,
        // so we can make sure that the process completes
        if (!this.correlator.isShuttingDown()) {
            this.timedCorrelationInProgress.set(true);
            try {
                ensureUpdatesProcessed(true);
            } finally {
                this.timedCorrelationInProgress.set(false);
            }
        }
    }
    
    public void ensureUpdatesProcessed(boolean scheduled) {
        if (this.correlator.isEnabled()) {
            List<QueryMetricUpdate> correlatedUpdates;
            do {
                correlatedUpdates = this.correlator.getMetricUpdates(QueryMetricOperations.inProcess);
                if (correlatedUpdates != null && !correlatedUpdates.isEmpty()) {
                    try {
                        String queryId = correlatedUpdates.get(0).getMetric().getQueryId();
                        QueryMetricType metricType = correlatedUpdates.get(0).getMetricType();
                        QueryMetricUpdateHolder metricUpdate = combineMetricUpdates(correlatedUpdates, metricType);
                        log.debug("storing correlated updates for {}", queryId);
                        storeMetricUpdate(metricUpdate);
                    } catch (Exception e) {
                        log.error("exception while combining correlated updates: " + e.getMessage(), e);
                    }
                }
            } while (!(scheduled && this.correlator.isShuttingDown()) && correlatedUpdates != null && !correlatedUpdates.isEmpty());
        }
    }
    
    /**
     * Update metrics void response.
     *
     * @param queryMetrics
     *            the list of query metric updates
     * @param metricType
     *            the metric type
     * @return the void response
     */
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
    @Operation(summary = "Submit a list of metrics updates.", description = "Metrics updates will be placed on a message queue to ensure a quick response.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/updateMetrics", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetrics(@RequestBody List<BaseQueryMetric> queryMetrics,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        if (!this.mergeLock.isAllowedReadLock()) {
            throw new IllegalStateException("service unavailable");
        }
        for (BaseQueryMetric m : queryMetrics) {
            if (log.isTraceEnabled()) {
                log.trace("received bulk metric update via REST: " + m.toString());
            } else {
                log.debug("received bulk metric update via REST: " + m.getQueryId());
            }
        }
        Timer.Context restTimer = this.stats.getTimer(TIMERS.REST).time();
        try {
            // must use QueryMetricTransportType.MESSAGE to avoid a REST loop
            queryMetricClient.submit(new QueryMetricClient.Request.Builder().withMetrics(queryMetrics).withMetricType(metricType).build(),
                            QueryMetricTransportType.MESSAGE);
        } catch (Exception e) {
            throw new RuntimeException("Unable to process bulk query metric update");
        } finally {
            restTimer.stop();
        }
        return new VoidResponse();
    }
    
    /**
     * Update metric void response.
     *
     * @param queryMetric
     *            the query metric update
     * @param metricType
     *            the metric type
     * @return the void response
     */
    // Messages that arrive via http/https get placed on the message queue
    // to ensure a quick response and to maintain a single queue of work
    @Operation(summary = "Submit a single metric update.", description = "The metric update will be placed on a message queue to ensure a quick response.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/updateMetric", method = {RequestMethod.POST}, consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public VoidResponse updateMetric(@RequestBody BaseQueryMetric queryMetric,
                    @RequestParam(value = "metricType", defaultValue = "DISTRIBUTED") QueryMetricType metricType) {
        if (!this.mergeLock.isAllowedReadLock()) {
            throw new IllegalStateException("service unavailable");
        }
        Timer.Context restTimer = this.stats.getTimer(TIMERS.REST).time();
        try {
            if (log.isTraceEnabled()) {
                log.trace("received metric update via REST: " + queryMetric.toString());
            } else {
                log.debug("received metric update via REST: " + queryMetric.getQueryId());
            }
            try {
                // must use QueryMetricTransportType.MESSAGE to avoid a REST loop and not rely on the client configuration
                queryMetricClient.submit(new QueryMetricClient.Request.Builder().withMetric(queryMetric).withMetricType(metricType).build(),
                                QueryMetricTransportType.MESSAGE);
            } catch (Exception e) {
                throw new RuntimeException("Unable to process query metric update for query [" + queryMetric.getQueryId() + "]");
            }
        } finally {
            restTimer.stop();
        }
        return new VoidResponse();
    }
    
    private String getClusterLocalMemberUuid() {
        return ((HazelcastCacheManager) this.cacheManager).getHazelcastInstance().getCluster().getLocalMember().getUuid().toString();
    }
    
    public QueryMetricUpdateHolder combineMetricUpdates(List<QueryMetricUpdate> updates, QueryMetricType metricType) throws Exception {
        BaseQueryMetric combinedMetric = null;
        Lifecycle lowestLifecycle = null;
        for (QueryMetricUpdate u : updates) {
            if (combinedMetric == null) {
                combinedMetric = u.getMetric();
                lowestLifecycle = u.getMetric().getLifecycle();
            } else {
                if (u.getMetric().getLifecycle().ordinal() < lowestLifecycle.ordinal()) {
                    lowestLifecycle = u.getMetric().getLifecycle();
                }
                combinedMetric = this.handler.combineMetrics(u.getMetric(), combinedMetric, metricType);
            }
        }
        QueryMetricUpdateHolder metricUpdateHolder = new QueryMetricUpdateHolder(combinedMetric, metricType);
        metricUpdateHolder.updateLowestLifecycle(lowestLifecycle);
        return metricUpdateHolder;
    }
    
    public void storeMetricUpdate(QueryMetricUpdateHolder metricUpdate) {
        Timer.Context storeTimer = this.stats.getTimer(TIMERS.STORE).time();
        String queryId = metricUpdate.getMetric().getQueryId();
        try {
            IMap<String,QueryMetricUpdateHolder> incomingQueryMetricsCacheHz = ((IMap<String,QueryMetricUpdateHolder>) incomingQueryMetricsCache
                            .getNativeCache());
            if (metricUpdate.getMetric().getPositiveSelectors() == null) {
                this.handler.populateMetricSelectors(metricUpdate.getMetric());
            }
            this.mergeLock.lock();
            QueryMetricOperations.inProcess.add(queryId);
            try {
                incomingQueryMetricsCacheHz.executeOnKey(queryId, this.entryProcessorFactory.createEntryProcessor(metricUpdate));
            } finally {
                QueryMetricOperations.inProcess.remove(queryId);
                this.mergeLock.unlock();
            }
        } catch (Exception e) {
            if (!this.mergeLock.isShuttingDown()) {
                if (e instanceof HazelcastInstanceNotActiveException) {
                    log.error("HazelcastInstanceNotActiveException - OK if shutting down");
                } else {
                    log.error(e.getMessage(), e);
                }
            }
            // fail the handling of the message
            throw new RuntimeException(e.getMessage());
        } finally {
            storeTimer.stop();
        }
    }
    
    public static Set<String> getInProcess() {
        return inProcess;
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return the base query metric list response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get the metrics for a given query ID.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public BaseQueryMetricListResponse query(@AuthenticationPrincipal DatawaveUserDetails currentUser,
                    @Parameter(description = "queryId to return") @PathVariable("queryId") String queryId) {
        
        BaseQueryMetricListResponse response = queryMetricResponseFactory.createListResponse();
        List<BaseQueryMetric> metricList = new ArrayList<>();
        try {
            List<BaseQueryMetric> metricsFromAccumulo = this.handler.getQueryMetrics(queryId);
            for (BaseQueryMetric metric : metricsFromAccumulo) {
                boolean allowAllMetrics = false;
                boolean sameUser = false;
                if (currentUser != null) {
                    String metricUser = metric.getUser();
                    String requestingUser = dnUtils.getShortName(currentUser.getPrimaryUser().getName());
                    sameUser = metricUser != null && metricUser.equals(requestingUser);
                    allowAllMetrics = currentUser.getPrimaryUser().getRoles().contains("MetricsAdministrator");
                }
                // since we are using the incomingQueryMetricsCache, we need to make sure that the
                // requesting user has the necessary Authorizations to view the requested query metric
                if (sameUser || allowAllMetrics) {
                    ColumnVisibility columnVisibility = this.markingFunctions.translateToColumnVisibility(metric.getMarkings());
                    boolean userCanSeeVisibility = true;
                    for (DatawaveUser user : currentUser.getProxiedUsers()) {
                        Authorizations authorizations = new Authorizations(user.getAuths().toArray(new String[0]));
                        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(authorizations);
                        if (visibilityEvaluator.evaluate(columnVisibility) == false) {
                            userCanSeeVisibility = false;
                            break;
                        }
                    }
                    if (userCanSeeVisibility) {
                        // if returning this metric, then add any updates found in the incomingQueryMetricsCache
                        metricList.add(completeMetric(metric));
                    }
                }
            }
        } catch (Exception e) {
            response.addException(new QueryException(e.getMessage(), 500));
        }
        // Set the result to have the formatted query and query plan
        // StackOverflowErrors seen in JexlFormattedStringBuildingVisitor.formatMetrics, so protect
        // this call for each metric with try/catch and add original metric if formatMetrics fails
        List<BaseQueryMetric> fmtMetricList = new ArrayList<>();
        for (BaseQueryMetric m : metricList) {
            List<BaseQueryMetric> formatted = null;
            try {
                formatted = JexlFormattedStringBuildingVisitor.formatMetrics(Collections.singletonList(m));
            } catch (StackOverflowError | Exception e) {
                log.warn(String.format("%s while formatting metric %s: %s", e.getClass().getCanonicalName(), m.getQueryId(), e.getMessage()));
            }
            if (formatted == null || formatted.isEmpty()) {
                fmtMetricList.add(m);
            } else {
                fmtMetricList.addAll(formatted);
            }
        }
        response.setResult(fmtMetricList);
        if (fmtMetricList.isEmpty()) {
            response.setHasResults(false);
        } else {
            response.setGeoQuery(fmtMetricList.stream().anyMatch(SimpleQueryGeometryHandler::isGeoQuery));
            response.setHasResults(true);
        }
        return response;
    }
    
    /**
     * Returns metrics for the current users queries that are identified by the id
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return the ModelAndView for the webpage
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get the metrics for a given query ID.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}", method = {RequestMethod.GET}, produces = {MediaType.TEXT_HTML_VALUE})
    public ModelAndView queryWebpage(@AuthenticationPrincipal DatawaveUserDetails currentUser,
                    @Parameter(description = "queryId to return") @PathVariable("queryId") String queryId,
                    @Parameter(description = "queryId to return") @RequestParam(name = "display", required = false) String display) {
        
        BaseQueryMetricListResponse response = query(currentUser, queryId);
        if (StringUtils.isNotBlank(display) && display.equalsIgnoreCase("horizontal")) {
            response.setViewName("querymetric-horizontal");
        }
        return response.createModelAndView();
    }
    
    /**
     * Get a map for the given query represented by the query ID, if applicable.
     *
     * @param currentUser
     *            the current user
     * @param queryId
     *            the query id
     * @return the query geometry response
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a map for the given query represented by the query ID, if applicable.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}/map", method = {RequestMethod.GET, RequestMethod.POST},
                    produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public QueryGeometryResponse map(@AuthenticationPrincipal DatawaveUserDetails currentUser, @PathVariable("queryId") String queryId) {
        QueryGeometryResponse queryGeometryResponse = queryMetricResponseFactory.createGeoResponse();
        BaseQueryMetricListResponse metricResponse = query(currentUser, queryId);
        if (metricResponse.getExceptions() == null || metricResponse.getExceptions().isEmpty()) {
            return geometryHandler.getQueryGeometryResponse(queryId, metricResponse.getResult());
        } else {
            metricResponse.getExceptions().forEach(e -> queryGeometryResponse.addException(new QueryException(e.getMessage(), e.getCause(), e.getCode())));
            return queryGeometryResponse;
        }
    }
    
    /*
     * Pages could be missing if the metric was updated since being written to Accumulo
     */
    protected BaseQueryMetric completeMetric(BaseQueryMetric metricFromAccumulo) {
        String queryId = metricFromAccumulo.getQueryId();
        QueryMetricUpdateHolder metricUpdateHolderFromCache = incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class);
        BaseQueryMetric updatedMetric = metricFromAccumulo;
        if (metricUpdateHolderFromCache != null) {
            try {
                BaseQueryMetric metricFromCache = metricUpdateHolderFromCache.getMetric();
                QueryMetricType metricType = metricUpdateHolderFromCache.getMetricType();
                if (metricType.equals(QueryMetricType.DISTRIBUTED)) {
                    // These values are additive, so we don't want to count them twice
                    // The QueryMetricUpdateHolder resets the values when written to Accumulo
                    metricFromCache.setSourceCount(metricUpdateHolderFromCache.getValue("sourceCount"));
                    metricFromCache.setNextCount(metricUpdateHolderFromCache.getValue("nextCount"));
                    metricFromCache.setSeekCount(metricUpdateHolderFromCache.getValue("seekCount"));
                    metricFromCache.setYieldCount(metricUpdateHolderFromCache.getValue("yieldCount"));
                    metricFromCache.setDocSize(metricUpdateHolderFromCache.getValue("docSize"));
                    metricFromCache.setDocRanges(metricUpdateHolderFromCache.getValue("docRanges"));
                    metricFromCache.setFiRanges(metricUpdateHolderFromCache.getValue("fiRanges"));
                }
                updatedMetric = this.handler.combineMetrics(metricFromCache, metricFromAccumulo, metricUpdateHolderFromCache.getMetricType());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return updatedMetric;
    }
    
    @Operation(summary = "Get a map for the given query represented by the query ID, if applicable.")
    @PermitAll
    @RequestMapping(path = "/id/{queryId}/map", method = {RequestMethod.GET, RequestMethod.POST}, produces = {MediaType.TEXT_HTML_VALUE})
    public ModelAndView mapWebpage(@AuthenticationPrincipal DatawaveUserDetails currentUser, @PathVariable("queryId") String queryId) {
        return map(currentUser, queryId).createModelAndView();
    }
    
    private static Date parseDate(String dateString, DEFAULT_DATETIME defaultDateTime) throws IllegalArgumentException {
        if (StringUtils.isBlank(dateString)) {
            return null;
        } else {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                dateString = dateString.trim();
                if (dateString.length() == 8) {
                    dateString = dateString + (defaultDateTime.equals(BEGIN) ? " 000000.000" : " 235959.999");
                } else if (dateString.length() == 15) {
                    dateString = dateString + (defaultDateTime.equals(BEGIN) ? ".000" : ".999");
                }
                return sdf.parse(dateString);
            } catch (ParseException e) {
                String parameter = defaultDateTime.equals(BEGIN) ? "begin" : "end";
                throw new IllegalArgumentException(
                                "Unable to parse parameter '" + parameter + "' - valid formats: yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS");
            }
        }
    }
    
    private QueryMetricsSummaryResponse queryMetricsSummary(Date begin, Date end, DatawaveUserDetails currentUser, boolean onlyCurrentUser) {
        
        if (null == end) {
            end = new Date();
        }
        Calendar ninetyDaysBeforeEnd = Calendar.getInstance();
        ninetyDaysBeforeEnd.setTime(end);
        ninetyDaysBeforeEnd.add(Calendar.DATE, -90);
        if (null == begin) {
            // ninety days before end
            begin = ninetyDaysBeforeEnd.getTime();
        }
        QueryMetricsSummaryResponse response;
        if (end.before(begin)) {
            response = queryMetricResponseFactory.createSummaryResponse();
            String s = "begin date can not be after end date";
            response.addException(new QueryException(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE, new IllegalArgumentException(s), s));
        } else {
            response = handler.getQueryMetricsSummary(begin, end, currentUser, onlyCurrentUser);
        }
        return response;
    }
    
    /**
     * Returns a summary of the query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/summary/all", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public QueryMetricsSummaryResponse getQueryMetricsSummary(@AuthenticationPrincipal DatawaveUserDetails currentUser,
                    @RequestParam(required = false) String begin, @RequestParam(required = false) String end) {
        
        try {
            return queryMetricsSummary(parseDate(begin, BEGIN), parseDate(end, END), currentUser, false);
        } catch (Exception e) {
            QueryMetricsSummaryResponse response = queryMetricResponseFactory.createSummaryResponse();
            response.addException(e);
            return response;
        }
    }
    
    /**
     * Returns a summary of the query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/summary/all", method = {RequestMethod.GET}, produces = {MediaType.TEXT_HTML_VALUE})
    public ModelAndView getQueryMetricsSummaryWebpage(@AuthenticationPrincipal DatawaveUserDetails currentUser, @RequestParam(required = false) String begin,
                    @RequestParam(required = false) String end) {
        
        return getQueryMetricsSummary(currentUser, begin, end).createModelAndView();
    }
    
    /**
     * Returns a summary of the requesting user's query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics user summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics for the given user.")
    @PermitAll
    @RequestMapping(path = "/summary/user", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public QueryMetricsSummaryResponse getQueryMetricsUserSummary(@AuthenticationPrincipal DatawaveUserDetails currentUser,
                    @RequestParam(required = false) String begin, @RequestParam(required = false) String end) {
        try {
            return queryMetricsSummary(parseDate(begin, BEGIN), parseDate(end, END), currentUser, true);
        } catch (Exception e) {
            QueryMetricsSummaryResponse response = queryMetricResponseFactory.createSummaryResponse();
            response.addException(e);
            return response;
        }
    }
    
    /**
     * Returns a summary of the requesting user's query metrics
     *
     * @param currentUser
     *            the current user
     * @param begin
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @param end
     *            formatted date/time (yyyyMMdd | yyyyMMdd HHmmss | yyyyMMdd HHmmss.SSS)
     * @return the query metrics user summary
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get a summary of the query metrics for the given user.")
    @PermitAll
    @RequestMapping(path = "/summary/user", method = {RequestMethod.GET}, produces = {MediaType.TEXT_HTML_VALUE})
    public ModelAndView getQueryMetricsUserSummaryWebpage(@AuthenticationPrincipal DatawaveUserDetails currentUser,
                    @RequestParam(required = false) String begin, @RequestParam(required = false) String end) {
        
        return getQueryMetricsUserSummary(currentUser, begin, end).createModelAndView();
    }
    
    /**
     * Returns cache stats for the local part of the distributed Hazelcast cache
     *
     * @return the cache stats
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Get the query metrics cache stats.")
    @Secured({"Administrator", "JBossAdministrator", "MetricsAdministrator"})
    @RequestMapping(path = "/cacheStats", method = {RequestMethod.GET}, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public CacheStats getCacheStats() {
        CacheStats cacheStats = new CacheStats();
        IMap<Object,Object> incomingCacheHz = ((IMap<Object,Object>) incomingQueryMetricsCache.getNativeCache());
        cacheStats.setIncomingQueryMetrics(this.stats.getLocalMapStats(incomingCacheHz.getLocalMapStats()));
        cacheStats.setServiceStats(this.stats.formatStats(this.stats.getServiceStats(), true));
        cacheStats.setMemberUuid(getClusterLocalMemberUuid());
        try {
            cacheStats.setHost(InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            
        }
        return cacheStats;
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.logServiceStatsRateMs:300000}")
    public void logStats() {
        this.stats.logServiceStats();
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.publishServiceStatsToTimelyRateMs:60000}")
    public void publishServiceStatsToTimely() {
        this.stats.writeServiceStatsToTimely();
    }
    
    @Scheduled(fixedRateString = "${datawave.query.metric.stats.publishQueryStatsToTimelyRateMs:60000}")
    public void publishQueryStatsToTimely() {
        this.stats.queueAggregatedQueryStatsForTimely();
        this.stats.writeQueryStatsToTimely();
    }
}

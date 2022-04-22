package datawave.webservice.query.runner;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.util.AuthorizationsUtil;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.AbstractRunningQuery;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.WritesQueryMetrics;
import datawave.webservice.query.logic.WritesResultCardinalities;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.jboss.logging.NDC;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Object that encapsulates a running query
 *
 */
public class RunningQuery extends AbstractRunningQuery implements Runnable {
    
    private static final long serialVersionUID = 1L;
    
    private static Logger log = Logger.getLogger(RunningQuery.class);
    
    private transient Connector connection = null;
    private AccumuloConnectionFactory.Priority connectionPriority = null;
    private transient QueryLogic<?> logic = null;
    private Query settings = null;
    private long numResults = 0;
    private long lastPageNumber = 0;
    private transient TransformIterator iter = null;
    private Set<Authorizations> calculatedAuths = null;
    private boolean finished = false;
    private volatile boolean canceled = false;
    private TInfo traceInfo = null;
    private transient QueryMetricsBean queryMetrics = null;
    private RunningQueryTiming timing = null;
    private ExecutorService executor = null;
    private volatile Future<Object> future = null;
    private QueryPredictor predictor = null;
    private long maxResults = 0;
    
    public RunningQuery() {
        super(new QueryMetricFactoryImpl());
    }
    
    public RunningQuery(Connector connection, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(null, connection, priority, logic, settings, methodAuths, principal, null, null, metricFactory);
    }
    
    public RunningQuery(Connector connection, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, RunningQueryTiming timing, ExecutorService executor, QueryMetricFactory metricFactory) throws Exception {
        this(null, connection, priority, logic, settings, methodAuths, principal, timing, executor, metricFactory);
    }
    
    public RunningQuery(QueryMetricsBean queryMetrics, Connector connection, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(queryMetrics, connection, priority, logic, settings, methodAuths, principal, null, null, metricFactory);
    }
    
    public RunningQuery(QueryMetricsBean queryMetrics, Connector connection, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, ExecutorService executor, QueryMetricFactory metricFactory)
                    throws Exception {
        this(queryMetrics, connection, priority, logic, settings, methodAuths, principal, timing, executor, null, metricFactory);
    }
    
    public RunningQuery(QueryMetricsBean queryMetrics, Connector connection, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, ExecutorService executor, QueryPredictor predictor,
                    QueryMetricFactory metricFactory) throws Exception {
        super(metricFactory);
        if (logic != null && logic.getCollectQueryMetrics()) {
            this.queryMetrics = queryMetrics;
        }
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
        this.logic = logic;
        this.connectionPriority = priority;
        this.settings = settings;
        this.calculatedAuths = AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, principal);
        this.timing = timing;
        this.executor = executor;
        this.predictor = predictor;
        // set the metric information
        this.getMetric().populate(this.settings);
        this.getMetric().setQueryType(this.getClass().getSimpleName());
        if (this.queryMetrics != null) {
            try {
                this.queryMetrics.updateMetric(this.getMetric());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        // If connection is null, then we are likely not going to use this object for query, probably for removing or closing it.
        if (null != connection) {
            setConnection(connection);
        }
        this.maxResults = this.logic.getResultLimit(this.settings.getDnList());
        if (this.maxResults != this.logic.getMaxResults()) {
            log.info("Maximum results set to " + this.maxResults + " instead of default " + this.logic.getMaxResults() + ", user " + this.settings.getUserDN()
                            + " has a DN configured with a different limit");
        }
    }
    
    public static RunningQuery createQueryWithAuthorizations(QueryMetricsBean queryMetrics, Connector connection, AccumuloConnectionFactory.Priority priority,
                    QueryLogic<?> logic, Query settings, String methodAuths, RunningQueryTiming timing, ExecutorService executor, QueryPredictor predictor,
                    QueryMetricFactory metricFactory) throws Exception {
        RunningQuery runningQuery = new RunningQuery(queryMetrics, connection, priority, logic, settings, methodAuths, null, timing, executor, predictor,
                        metricFactory);
        runningQuery.calculatedAuths = Collections.singleton(new Authorizations(methodAuths));
        return runningQuery;
    }
    
    private void addNDC() {
        String user = this.settings.getUserDN();
        UUID uuid = this.settings.getId();
        if (user != null && uuid != null) {
            NDC.push("[" + user + "] [" + uuid + "]");
        }
    }
    
    private void removeNDC() {
        NDC.pop();
    }
    
    public void setConnection(Connector connection) throws Exception {
        // if we are setting this null, we shouldn't try to initialize
        // the internal logic
        if (connection == null) {
            this.connection = null;
            return;
        }
        
        try {
            addNDC();
            applyPrediction(null);
            this.connection = connection;
            long start = System.currentTimeMillis();
            GenericQueryConfiguration configuration = this.logic.initialize(this.connection, this.settings, this.calculatedAuths);
            this.lastPageNumber = 0;
            this.logic.setupQuery(configuration);
            this.iter = this.logic.getTransformIterator(this.settings);
            // the configuration query string should now hold the planned query
            this.getMetric().setPlan(configuration.getQueryString());
            this.getMetric().setSetupTime((System.currentTimeMillis() - start));
            this.getMetric().setLifecycle(QueryMetric.Lifecycle.INITIALIZED);
            testForUncaughtException(0);
            // TODO: applyPrediction("Plan");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            if (this.logic instanceof BaseQueryLogic && this.logic.getCollectQueryMetrics() && null != this.getMetric()) {
                GenericQueryConfiguration config = ((BaseQueryLogic) this.logic).getConfig();
                if (null != config) {
                    this.getMetric().setPlan(config.getQueryString());
                }
            }
            this.getMetric().setError(e);
            throw e;
        } finally {
            // update AbstractRunningQuery.lastUsed in case this operation took a long time
            touch();
            removeNDC();
            if (this.queryMetrics != null) {
                try {
                    this.queryMetrics.updateMetric(this.getMetric());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
    
    public ResultsPage next() throws Exception {
        // update AbstractRunningQuery.lastUsed
        touch();
        long pageStartTime = System.currentTimeMillis();
        List<Object> resultList = new ArrayList<>();
        boolean hitPageByteTrigger = false;
        boolean hitPageTimeTrigger = false;
        try {
            addNDC();
            int currentPageCount = 0;
            long currentPageBytes = 0;
            
            // test for any exceptions prior to loop as hasNext() would likely be false;
            testForUncaughtException(resultList.size());
            
            while (!this.finished && ((future != null) || this.iter.hasNext())) {
                // if we are canceled, then break out
                if (this.canceled) {
                    log.info("Query has been cancelled, aborting query.next call");
                    this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
                    break;
                }
                // if the number of results has reached out page size, then break out
                if (currentPageCount >= this.settings.getPagesize()) {
                    log.info("Query requested page size had been reached, aborting query.next call");
                    break;
                }
                // if the logic had a max page size and we have reached that, then break out
                if (this.logic.getMaxPageSize() > 0 && currentPageCount >= this.logic.getMaxPageSize()) {
                    log.info("Query logic max page size has been reached, aborting query.next call");
                    break;
                }
                // if the logic had a page byte trigger and we have readed that, then break out
                if (this.logic.getPageByteTrigger() > 0 && currentPageBytes >= this.logic.getPageByteTrigger()) {
                    log.info("Query logic max page byte trigger has been reached, aborting query.next call");
                    hitPageByteTrigger = true;
                    break;
                }
                // if the logic had a max num results (across all pages) and we have reached that (or the maxResultsOverride if set), then break out
                if (this.settings.isMaxResultsOverridden()) {
                    if (this.settings.getMaxResultsOverride() >= 0 && numResults >= this.settings.getMaxResultsOverride()) {
                        log.info("Max results override has been reached, aborting query.next call");
                        this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXRESULTS);
                        break;
                    }
                } else if (this.maxResults >= 0 && numResults >= this.maxResults) {
                    log.info("Query logic max results has been reached, aborting query.next call");
                    this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXRESULTS);
                    break;
                }
                if (this.logic.getMaxWork() >= 0 && (this.getMetric().getNextCount() + this.getMetric().getSeekCount()) >= this.logic.getMaxWork()) {
                    log.info("Query logic max work has been reached, aborting query.next call");
                    this.getMetric().setLifecycle(QueryMetric.Lifecycle.MAXWORK);
                    break;
                }
                // if we are the specified amount on the way to timing out on this call and we have results,
                // determine whether we are on track to having enough results
                // use the pagestart time for the time in call since we only care about the execution time of
                // this page.
                long pageTimeInCall = (System.currentTimeMillis() - pageStartTime);
                
                int maxPageSize = Math.min(this.settings.getPagesize(), this.logic.getMaxPageSize());
                if (timing != null && currentPageCount > 0 && timing.shouldReturnPartialResults(currentPageCount, maxPageSize, pageTimeInCall)) {
                    log.info("Query logic max expire before page is full, returning existing results " + currentPageCount + " " + maxPageSize + " "
                                    + pageTimeInCall + " " + timing);
                    hitPageTimeTrigger = true;
                    break;
                }
                
                Object o = null;
                if (executor != null) {
                    if (future == null) {
                        future = executor.submit(() -> iter.next());
                    }
                    try {
                        o = future.get(1, TimeUnit.MINUTES);
                        future = null;
                    } catch (InterruptedException ie) {
                        // in this case we were most likely cancelled, no longer waiting
                        future = null;
                    } catch (ExecutionException ee) {
                        // in this case we need to pass up the exception
                        future = null;
                        throw ee;
                    } catch (TimeoutException te) {
                        // in this case we are still waiting on our future....simply continue
                    }
                } else {
                    o = iter.next();
                }
                
                // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
                if (iter.getTransformer() instanceof WritesQueryMetrics) {
                    ((WritesQueryMetrics) iter.getTransformer()).writeQueryMetrics(this.getMetric());
                }
                
                // if not still waiting on a future, then process the result (or lack thereof)
                if (future == null) {
                    if (null == o) {
                        log.debug("Null result encountered, no more results");
                        this.finished = true;
                        break;
                    }
                    resultList.add(o);
                    if (this.logic.getPageByteTrigger() > 0) {
                        currentPageBytes += ObjectSizeOf.Sizer.getObjectSize(o);
                    }
                    currentPageCount++;
                    numResults++;
                }
                
                testForUncaughtException(resultList.size());
            }
            
            // if the last hasNext() call failed, then we would catch the exception here
            testForUncaughtException(resultList.size());
            
            // Update the metric
            long now = System.currentTimeMillis();
            this.getMetric().addPageTime(currentPageCount, now - pageStartTime, pageStartTime, now);
            this.lastPageNumber++;
            if (!resultList.isEmpty()) {
                this.getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            this.getMetric().setError(e);
            throw e;
        } finally {
            // update AbstractRunningQuery.lastUsed in case this operation took a long time
            touch();
            removeNDC();
            
            if (this.queryMetrics != null) {
                try {
                    this.queryMetrics.updateMetric(this.getMetric());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        if (resultList.isEmpty()) {
            return new ResultsPage();
        } else {
            return new ResultsPage(resultList, ((hitPageByteTrigger || hitPageTimeTrigger) ? ResultsPage.Status.PARTIAL : ResultsPage.Status.COMPLETE));
        }
    }
    
    public void cancel() {
        this.canceled = true;
        // save off the future as it could be removed at any time
        Future<Object> future = this.future;
        // cancel the future if we have one
        if (future != null) {
            future.cancel(true);
        }
        
        // change status to cancelled
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
    }
    
    public boolean isFinished() {
        return finished;
    }
    
    public boolean isCanceled() {
        return canceled;
    }
    
    public Connector getConnection() {
        return connection;
    }
    
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return connectionPriority;
    }
    
    public QueryLogic<?> getLogic() {
        return logic;
    }
    
    public Query getSettings() {
        return settings;
    }
    
    public TransformIterator getTransformIterator() {
        return iter;
    }
    
    protected Set<Authorizations> getCalculatedAuths() {
        return calculatedAuths;
    }
    
    protected QueryPredictor getPredictor() {
        return this.predictor;
    }
    
    public void setPredictor(QueryPredictor predictor) {
        this.predictor = predictor;
    }
    
    protected void applyPrediction(String context) {
        if (getPredictor() != null) {
            try {
                Set<Prediction> predictions = getPredictor().predict(this.getMetric());
                if (predictions != null) {
                    // now we have a predictions, lets broadcast
                    log.info("Model predictions: " + predictions);
                    context = (context == null ? "" : context + ' ');
                    for (Prediction prediction : predictions) {
                        // append the context to the predictions
                        this.getMetric().addPrediction(new Prediction(context + prediction.getName(), prediction.getPrediction()));
                    }
                }
            } catch (Exception e) {
                log.warn("Unable to apply query prediction", e);
            }
        }
    }
    
    public void closeConnection(AccumuloConnectionFactory factory) throws Exception {
        this.getMetric().setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
        
        if (iter != null && iter.getTransformer() instanceof WritesResultCardinalities) {
            ((WritesResultCardinalities) iter.getTransformer()).writeResultCardinalities();
        }
        
        if (connection != null) {
            try {
                factory.returnConnection(connection);
                connection = null;
            } finally {
                // only push metrics if this RunningQuery was initialized
                if (this.queryMetrics != null) {
                    try {
                        queryMetrics.updateMetric(this.getMetric());
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
            }
        }
        
        if (logic != null) {
            try {
                addNDC();
                logic.close();
            } catch (Exception e) {
                log.error("Exception occurred while closing query logic; may be innocuous if scanners were running.", e);
            } finally {
                removeNDC();
            }
        }
    }
    
    @Override
    public long getLastPageNumber() {
        return this.lastPageNumber;
    }
    
    @Override
    public String toString() {
        
        String host = System.getProperty("jboss.host.name");
        
        return new StringBuilder().append("host:").append(host).append(", id:").append(this.getSettings().getId()).append(", query:")
                        .append(StringEscapeUtils.escapeHtml(this.getSettings().getQuery())).append(", auths:")
                        .append(this.getSettings().getQueryAuthorizations()).append(", user:").append(this.getSettings().getOwner()).append(", queryLogic:")
                        .append(this.getSettings().getQueryLogicName()).append(", name:").append(this.getSettings().getQueryName()).append(", pagesize:")
                        .append(this.getSettings().getPagesize()).append(", begin:").append(this.getSettings().getBeginDate()).append(", end:")
                        .append(this.getSettings().getEndDate()).append(", expiration:").append(this.getSettings().getExpirationDate()).append(", params: ")
                        .append(this.getSettings().getParameters()).append(", callTime: ")
                        .append((this.getTimeOfCurrentCall() == 0) ? 0 : System.currentTimeMillis() - this.getTimeOfCurrentCall()).toString();
        
    }
    
    /**
     * Sets {@link TInfo} for this query as an indication that the query is being traced. This trace info is also used to continue a trace across different
     * thread boundaries.
     */
    public void setTraceInfo(TInfo traceInfo) {
        this.traceInfo = traceInfo;
    }
    
    /**
     * Gets the {@link TInfo} associated with this query, if any. If the query is not being traced, then {@code null} is returned. Callers can continue a trace
     * on a different thread by calling {@link org.apache.accumulo.core.trace.Trace#trace(TInfo, String)} with the info returned here, and then interacting with
     * the returned {@link org.apache.accumulo.core.trace.Span}.
     * 
     * @return the {@link TInfo} associated with this query, if any
     */
    public TInfo getTraceInfo() {
        return traceInfo;
    }
    
    public QueryMetricsBean getQueryMetrics() {
        return queryMetrics;
    }
    
    public void setQueryMetrics(QueryMetricsBean queryMetrics) {
        if (logic != null && logic.getCollectQueryMetrics() == true) {
            this.queryMetrics = queryMetrics;
        }
    }
    
    /**
     * An interface used to force returning from a next call within a running query.
     */
    public interface RunningQueryTiming {
        boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall);
    }
    
    /**
     * A noop implementation of the running query timing interface.
     */
    public static class RunningQueryTimingNoOp implements RunningQueryTiming {
        public boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall) {
            return false;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub
        
    }
    
    private void testForUncaughtException(int numResults) throws QueryException {
        QueryUncaughtExceptionHandler handler = settings.getUncaughtExceptionHandler();
        if (handler != null) {
            if (handler.getThrowable() != null) {
                if (numResults > 0) {
                    log.warn("Exception with Partial Results: resultList.getResults().size() is " + numResults + ", and there was an UncaughtException:"
                                    + handler.getThrowable() + " in thread " + handler.getThread());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Throwing:" + handler.getThrowable() + " for query with no results");
                    }
                }
                if (handler.getThrowable() instanceof QueryException) {
                    throw ((QueryException) handler.getThrowable());
                }
                throw new QueryException(handler.getThrowable());
            }
        }
    }
}

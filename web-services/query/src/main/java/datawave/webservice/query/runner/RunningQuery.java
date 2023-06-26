package datawave.webservice.query.runner;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.core.query.logic.WritesResultCardinalities;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.AbstractRunningQuery;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.jboss.logging.NDC;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Object that encapsulates a running query
 *
 */
public class RunningQuery extends AbstractRunningQuery implements Runnable {

    private static final long serialVersionUID = 1L;

    private static Logger log = Logger.getLogger(RunningQuery.class);

    private transient AccumuloClient client = null;
    private AccumuloConnectionFactory.Priority connectionPriority = null;
    private transient QueryLogic<?> logic = null;
    private Query settings = null;
    private long numResults = 0;
    private long lastPageNumber = 0;
    private transient TransformIterator iter = null;
    private Set<Authorizations> calculatedAuths = null;
    private boolean finished = false;
    private volatile boolean canceled = false;
    private transient QueryMetricsBean queryMetrics = null;
    private transient RunningQueryTiming timing = null;
    private ExecutorService executor = null;
    private volatile Future<Object> future = null;
    private final BlockingQueue<Object> resultsThreadQueue = new ArrayBlockingQueue<>(1);
    private final AtomicInteger hasNext = new AtomicInteger(0);
    private final AtomicInteger gotNext = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private QueryPredictor predictor = null;
    private long maxResults = 0;
    private int currentTimeoutcount = 0;
    private boolean allowShortCircuitTimeouts = false;

    public RunningQuery() {
        super(new QueryMetricFactoryImpl());
    }

    public RunningQuery(AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(null, client, priority, logic, settings, methodAuths, principal, null, null, metricFactory);
    }

    public RunningQuery(AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings, String methodAuths,
                    Principal principal, RunningQueryTiming timing, QueryMetricFactory metricFactory) throws Exception {
        this(null, client, priority, logic, settings, methodAuths, principal, timing, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, QueryMetricFactory metricFactory) throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryMetricFactory metricFactory) throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, timing, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryPredictor predictor, QueryMetricFactory metricFactory)
                    throws Exception {
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, timing, null, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryPredictor predictor, UserOperations userOperations,
                    QueryMetricFactory metricFactory) throws Exception {
        super(metricFactory);
        if (logic != null && logic.getCollectQueryMetrics()) {
            this.queryMetrics = queryMetrics;
        }
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
        this.logic = logic;
        this.connectionPriority = priority;
        this.settings = settings;
        // the query principal is our local principal unless the query logic has a different user operations
        DatawavePrincipal queryPrincipal = (DatawavePrincipal) ((logic.getUserOperations() == null) ? principal
                        : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal));
        // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
        DatawavePrincipal overallPrincipal = (DatawavePrincipal) ((userOperations == null) ? principal
                        : userOperations.getRemoteUser((DatawavePrincipal) principal));
        this.calculatedAuths = WSAuthorizationsUtil.getDowngradedAuthorizations(methodAuths, overallPrincipal, queryPrincipal);
        this.timing = timing;
        this.executor = Executors.newSingleThreadExecutor();
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
        if (null != client) {
            setClient(client);
        }

        this.maxResults = this.logic.getResultLimit(this.settings);
        if (this.maxResults != this.logic.getMaxResults()) {
            log.info("Maximum results set to " + this.maxResults + " instead of default " + this.logic.getMaxResults() + ", user " + this.settings.getUserDN()
                            + " has a DN configured with a different limit");
        }
    }

    public static RunningQuery createQueryWithAuthorizations(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority,
                    QueryLogic<?> logic, Query settings, String methodAuths, RunningQueryTiming timing, QueryPredictor predictor,
                    QueryMetricFactory metricFactory) throws Exception {
        RunningQuery runningQuery = new RunningQuery(queryMetrics, client, priority, logic, settings, methodAuths, null, timing, predictor, metricFactory);
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

    public void setClient(AccumuloClient client) throws Exception {
        // if we are setting this null, we shouldn't try to initialize
        // the internal logic
        if (client == null) {
            this.client = null;
            return;
        }

        try {
            addNDC();
            applyPrediction(null);
            this.client = client;
            long start = System.currentTimeMillis();
            GenericQueryConfiguration configuration = this.logic.initialize(this.client, this.settings, this.calculatedAuths);
            this.lastPageNumber = 0;
            this.logic.setupQuery(configuration);
            this.iter = this.logic.getTransformIterator(this.settings);
            this.allowShortCircuitTimeouts = logic.isLongRunningQuery();
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

    /**
     * This is the results thread which will pull results from the iterator and add them to a blocking queue. The blocking queue will be of size 1 which means
     * that the main RunningQuery.next() loop will have to pull the results before the next one can be retrieved. The hasNext and gotNext counters keep track of
     * the calls to hasNext and next on the underlying iterator. They will be decremented once a result is acknowledged in the RunningQuery.next() loop. The
     * running boolean will allow the graceful termination of this thread.
     *
     * @return running (with a value of false)
     */
    private Object getResultsThread() {
        try {
            while (running.get() && !this.finished && !this.canceled && this.iter.hasNext()) {
                synchronized (hasNext) {
                    hasNext.incrementAndGet();
                    hasNext.notifyAll();
                }
                // wait until the queue is emptied
                while (running.get() && !this.finished && !this.canceled && !resultsThreadQueue.isEmpty()) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                // if the queue is available and we are still running, then get the next result
                if (running.get() && !this.finished && !this.canceled && resultsThreadQueue.isEmpty()) {
                    Object o = this.iter.next();
                    if (o != null) {
                        resultsThreadQueue.offer(o);
                        synchronized (gotNext) {
                            gotNext.incrementAndGet();
                            gotNext.notifyAll();
                        }
                    }

                    // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
                    if (iter.getTransformer() instanceof WritesQueryMetrics) {
                        ((WritesQueryMetrics) iter.getTransformer()).writeQueryMetrics(this.getMetric());
                    }
                }
            }
        } catch (Exception e) {
            if (settings.getUncaughtExceptionHandler() != null) {
                settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
            } else {
                running.set(false);
                synchronized (hasNext) {
                    hasNext.notifyAll();
                }
                synchronized (gotNext) {
                    gotNext.notifyAll();
                }
                throw new RuntimeException(e);
            }
        }

        running.set(false);
        synchronized (hasNext) {
            hasNext.notifyAll();
        }
        synchronized (gotNext) {
            gotNext.notifyAll();
        }

        return running;
    }

    /**
     * This method is used to determine if we have a next result. This will throw a timeout exception if the page short circuit limit is reached.
     *
     * @param pageStartTime
     *            the page start time
     * @return true if hasNext()
     * @throws TimeoutException
     *             if there is a timeout
     */
    private boolean hasNext(long pageStartTime) throws TimeoutException {
        if (allowShortCircuitTimeouts) {
            synchronized (hasNext) {
                if (hasNext.get() == 0 && running.get() && !this.finished && !this.canceled) {
                    long timeout = (timing != null ? Math.max(1, (timing.getPageShortCircuitTimeoutMs() - (System.currentTimeMillis() - pageStartTime)))
                                    : Long.MAX_VALUE);
                    try {
                        hasNext.wait(timeout);
                    } catch (InterruptedException e) {
                        // if we got interrupted, then just return false
                        return false;
                    }
                    if (running.get() && (hasNext.get() == 0)) {
                        throw new TimeoutException("hasNext timed out");
                    }
                }
                if (hasNext.get() == 0) {
                    log.debug("hasNext returned false.  No more results");
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            if (!this.finished && !this.canceled && this.iter.hasNext()) {
                hasNext.incrementAndGet();
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * This method will get the next object from the results thread queue. This presumes that hasNext has returned true. A timeout exception will be thrown if
     * the page short circuit timeout has been reached.
     *
     * @param pageStartTime
     *            the page start time
     * @return the next object (could be null)
     * @throws TimeoutException
     *             if there is a timeout
     */
    private Object getNext(long pageStartTime) throws TimeoutException {
        if (allowShortCircuitTimeouts) {
            synchronized (gotNext) {
                if (gotNext.get() == 0 && running.get() && !this.finished && !this.canceled) {
                    long timeout = (timing != null ? Math.max(1, (timing.getPageShortCircuitTimeoutMs() - (System.currentTimeMillis() - pageStartTime)))
                                    : Long.MAX_VALUE);
                    try {
                        gotNext.wait(timeout);
                    } catch (InterruptedException e) {
                        // if we got interrupted, then just return null
                        return null;
                    }
                    if (running.get() && (gotNext.get() == 0)) {
                        throw new TimeoutException("gotNext timed out");
                    }
                }
                return resultsThreadQueue.poll();
            }
        } else {
            Object o = iter.next();
            gotNext.incrementAndGet();

            // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
            if (iter.getTransformer() instanceof WritesQueryMetrics) {
                ((WritesQueryMetrics) iter.getTransformer()).writeQueryMetrics(this.getMetric());
            }

            return o;
        }
    }

    /**
     * terminate the results thread.
     */
    public void terminateResultsThread() {
        running.set(false);
        if (future != null) {
            future.cancel(false);
            while (!future.isDone()) {
                future.cancel(true);
            }
            future = null;
        }
        executor.shutdown();
    }

    /**
     * Get the next results page
     *
     * @return a results page.
     * @throws Exception
     *             if there are issues
     */
    public ResultsPage next() throws Exception {
        // update AbstractRunningQuery.lastUsed
        touch();
        long pageStartTime = System.currentTimeMillis();
        this.logic.setPageProcessingStartTime(pageStartTime);
        List<Object> resultList = new ArrayList<>();
        boolean hitPageByteTrigger = false;
        boolean hitPageTimeTrigger = false;
        boolean hitIntermediateResult = false;
        boolean hitShortCircuitForLongRunningQuery = false;
        try {
            addNDC();
            int currentPageCount = 0;
            long currentPageBytes = 0;

            // test for any exceptions prior to loop as hasNext() would likely be false;
            testForUncaughtException(resultList.size());

            // start up the results thread if needed
            if (this.allowShortCircuitTimeouts && future == null && !this.canceled && !this.finished) {
                running.set(true);
                future = executor.submit(() -> getResultsThread());
            }

            try {
                while (!this.finished && hasNext(pageStartTime)) {
                    // if we are canceled, then break out
                    if (this.canceled) {
                        log.info("Query has been cancelled, aborting query.next call");
                        this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
                        break;
                    }
                    // if the number of results has reached our page size, then break out
                    if (currentPageCount >= this.settings.getPagesize()) {
                        log.info("Query requested page size had been reached, aborting query.next call");
                        break;
                    }
                    // if the logic had a max page size, and we have reached that, then break out
                    if (this.logic.getMaxPageSize() > 0 && currentPageCount >= this.logic.getMaxPageSize()) {
                        log.info("Query logic max page size has been reached, aborting query.next call");
                        break;
                    }
                    // if the logic had a page byte trigger, and we have reached that, then break out
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

                    // now get the next object
                    Object o = getNext(pageStartTime);

                    // now that we got the next object, acknowledge via the counters
                    hasNext.decrementAndGet();
                    gotNext.decrementAndGet();

                    if (o instanceof EventBase && ((EventBase) o).isIntermediateResult()) {
                        log.info("Received an intermediate result");
                        // in this case we have timed out up stream somewhere, so lets return what we have
                        hitIntermediateResult = true;
                        break;
                    }

                    if (null == o) {
                        log.debug("Null result encountered, no more results");
                        this.finished = true;
                        terminateResultsThread();
                        break;
                    }

                    resultList.add(o);
                    if (this.logic.getPageByteTrigger() > 0) {
                        currentPageBytes += ObjectSizeOf.Sizer.getObjectSize(o);
                    }
                    currentPageCount++;
                    numResults++;

                    testForUncaughtException(resultList.size());
                }
            } catch (TimeoutException te) {
                log.info("Hit the timeout waiting for a result");
                // This means the iter.hasNext() call didn't return within the allotted time. If this is a long running query,
                // then we want to signal that the caller should call next to keep going (as opposed to just returning the
                // page as COMPLETE)
                if (allowShortCircuitTimeouts) {
                    log.info("Short circuiting the long running query");
                    hitShortCircuitForLongRunningQuery = true;
                } else if (resultList.isEmpty()) {
                    log.warn("Query timed out waiting for next result");
                    terminateResultsThread();
                    throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT, "Query timed out waiting for next result");
                }
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
            terminateResultsThread();
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

        if (!resultList.isEmpty()) {
            log.info("Returning page of results");
            // we have results!
            return new ResultsPage(resultList,
                            ((hitPageByteTrigger || hitPageTimeTrigger || hitIntermediateResult || hitShortCircuitForLongRunningQuery)
                                            ? ResultsPage.Status.PARTIAL
                                            : ResultsPage.Status.COMPLETE));
        } else {
            // we have no results. Let us determine whether we are done or not.

            // if we have hit an intermediate result or a short circuit then check to see how many times we hit this
            if (hitIntermediateResult || hitShortCircuitForLongRunningQuery) {
                currentTimeoutcount++;
                if (timing != null && currentTimeoutcount == timing.getMaxLongRunningTimeoutRetries()) {
                    log.warn("Query timed out waiting for results for too many ( " + currentTimeoutcount + ") cycles.");
                    terminateResultsThread();
                    // this means that we have timed out waiting for a result too many times over the course of this query.
                    // In this case we need to fail the next call with a timeout
                    throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT,
                                    "Query timed out waiting for results for too many (" + currentTimeoutcount + ") cycles.");
                } else {
                    log.info("Returning an empty partial results page");
                    // We are returning an empty page with a PARTIAL status to allow the query to continue running
                    return new ResultsPage(new ArrayList<>(), ResultsPage.Status.PARTIAL);
                }
            } else {
                log.info("Returning final empty page");
                terminateResultsThread();
                // This query is done, we have no more results to return.
                return new ResultsPage(Collections.emptyList(), ResultsPage.Status.NONE);
            }
        }
    }

    public void cancel() {
        this.canceled = true;

        terminateResultsThread();

        // change status to cancelled
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public AccumuloClient getClient() {
        return client;
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

        if (client != null) {
            try {
                factory.returnClient(client);
                client = null;
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

        int getMaxLongRunningTimeoutRetries();

        long getPageShortCircuitTimeoutMs();
    }

    /**
     * A noop implementation of the running query timing interface. -- only used by upstream tests
     */
    public static class RunningQueryTimingNoOp implements RunningQueryTiming {
        public boolean shouldReturnPartialResults(int pageSize, int maxPageSize, long timeInCall) {
            return false;
        }

        public int getMaxLongRunningTimeoutRetries() {
            return 0;
        }

        // hardcoded because only used by upstream tests.
        public long getPageShortCircuitTimeoutMs() {
            return 300000000000000L;
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

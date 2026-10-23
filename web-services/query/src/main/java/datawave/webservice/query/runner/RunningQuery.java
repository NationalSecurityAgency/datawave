package datawave.webservice.query.runner;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.jboss.logging.NDC;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.WritesResultCardinalities;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.authorization.remote.RemoteUserOperationsImpl;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.common.connection.WrappedAccumuloClient;
import datawave.webservice.query.cache.AbstractRunningQuery;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

/**
 * Object that encapsulates a running query
 *
 */
public class RunningQuery extends AbstractRunningQuery implements Runnable {

    private static final long serialVersionUID = -8812242917762274469L;

    private static Logger log = Logger.getLogger(RunningQuery.class);

    private transient AccumuloClient client = null;
    private AccumuloConnectionFactory.Priority connectionPriority = null;
    private transient QueryLogic<?> logic = null;
    private Query settings = null;
    private long numResults = 0;
    private volatile long lastPageNumber = 0;
    private volatile transient TransformIterator iter = null;
    private Set<Authorizations> calculatedAuths = null;
    private volatile boolean finished = false;
    private volatile boolean canceled = false;
    private transient QueryMetricsBean queryMetrics = null;
    private transient RunningQueryTiming timing = null;
    private transient ExecutorService executor = null;
    private transient volatile ResultsSource source;
    private transient volatile ResultsSource.PageRequest currentPage;
    private transient volatile Thread currentThread;
    private transient volatile Throwable terminalFailure;
    private final AtomicBoolean nextInProgress = new AtomicBoolean();
    private volatile int currentPageCount;
    private volatile boolean closed;
    private boolean initializing;
    private boolean hasExecution;
    private boolean logicClosed;
    private boolean cardinalitiesWritten;
    private transient QueryMetricFactory metricFactory;
    private QueryPredictor predictor = null;
    private long maxResults = 0;
    private int currentTimeoutcount = 0;
    private boolean allowIntermediateEmptyPages = false;
    private boolean useResultsThread = false;

    public RunningQuery() {
        super(new QueryMetricFactoryImpl());
        metricFactory = new QueryMetricFactoryImpl();
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
        this(queryMetrics, client, priority, logic, settings, methodAuths, principal, timing, predictor, null, metricFactory);
    }

    public RunningQuery(QueryMetricsBean queryMetrics, AccumuloClient client, AccumuloConnectionFactory.Priority priority, QueryLogic<?> logic, Query settings,
                    String methodAuths, Principal principal, RunningQueryTiming timing, QueryPredictor predictor, UserOperations userOperations,
                    QueryMetricFactory metricFactory) throws Exception {
        super(metricFactory);
        this.metricFactory = metricFactory;
        if (logic != null && logic.getCollectQueryMetrics()) {
            this.queryMetrics = queryMetrics;
        }
        this.getMetric().setLifecycle(QueryMetric.Lifecycle.DEFINED);
        this.logic = logic;
        this.connectionPriority = priority;
        this.settings = settings;
        // the query principal is our local principal unless the query logic has a different user operations
        if (methodAuths != null) {
            logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(WSAuthorizationsUtil.splitAuths(methodAuths))));
        } else {
            logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(null));
        }
        DatawavePrincipal queryPrincipal = (DatawavePrincipal) ((logic.getUserOperations() == null) ? principal
                        : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal));
        // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
        // don't call remote user operations if it's asked not to
        DatawavePrincipal overallPrincipal = (userOperations == null
                        || "false".equalsIgnoreCase(settings.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES).getParameterValue()))
                                        ? (DatawavePrincipal) principal
                                        : userOperations.getRemoteUser((DatawavePrincipal) principal);
        this.calculatedAuths = WSAuthorizationsUtil.getDowngradedAuthorizations(methodAuths, overallPrincipal, queryPrincipal);
        this.timing = timing;
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
        RunningQuery runningQuery = new RunningQuery(queryMetrics, null, priority, logic, settings, methodAuths, null, timing, predictor, metricFactory);
        runningQuery.calculatedAuths = WSAuthorizationsUtil
                        .buildAuthorizations(methodAuths == null ? null : Collections.singleton(WSAuthorizationsUtil.splitAuths(methodAuths)));
        if (client != null) {
            runningQuery.setClient(client);
        }
        return runningQuery;
    }

    private boolean addNDC() {
        String user = this.settings.getUserDN();
        UUID uuid = this.settings.getId();
        if (user != null && uuid != null) {
            NDC.push("[" + user + "] [" + uuid + "]");
            return true;
        }
        return false;
    }

    private void removeNDC(boolean pushed) {
        if (pushed) {
            NDC.pop();
        }
    }

    public synchronized void setClient(AccumuloClient client) throws Exception {
        // if we are setting this null, we shouldn't try to initialize
        // the internal logic
        if (client == null) {
            this.client = null;
            return;
        }

        synchronized (nextInProgress) {
            if (nextInProgress.get()) {
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Cannot reset during a next call");
            }
            if (this.client != null && hasExecution) {
                throw new IllegalStateException("Close the existing connection before resetting the query");
            }
            if (source != null) {
                source.cancel();
                if (!source.awaitStopped(5000)) {
                    throw new QueryException(DatawaveErrorCode.CLOSE_ERROR, "Previous results reader has not stopped");
                }
            }
            if (hasExecution) {
                settings.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
                if (metricFactory == null) {
                    metricFactory = new QueryMetricFactoryImpl();
                }
                this.metric = metricFactory.createMetric();
                this.metric.populate(settings);
                this.metric.setQueryType(getClass().getSimpleName());
                maxResults = logic.getResultLimit(settings);
            }
            hasExecution = true;
            source = null;
            iter = null;
            terminalFailure = null;
            finished = false;
            canceled = false;
            closed = false;
            logicClosed = false;
            cardinalitiesWritten = false;
            numResults = 0;
            lastPageNumber = 0;
            currentTimeoutcount = 0;
            initializing = true;
        }

        boolean ndcPushed = false;
        try {
            ndcPushed = addNDC();
            applyPrediction(null);
            this.client = client;
            if (this.client instanceof WrappedAccumuloClient && this.logic.getClientConfig() != null) {
                ((WrappedAccumuloClient) this.client).updateClientConfig(this.logic.getClientConfig());
            }
            long start = System.currentTimeMillis();
            GenericQueryConfiguration configuration = this.logic.initialize(this.client, this.settings, this.calculatedAuths);
            this.lastPageNumber = 0;
            this.logic.setupQuery(configuration);
            this.iter = this.logic.getTransformIterator(this.settings);
            this.allowIntermediateEmptyPages = logic.isLongRunningQuery();
            // force us to use asynchronous results thread to allow intermediate empty pages
            if (this.allowIntermediateEmptyPages) {
                this.useResultsThread = true;
            }
            // the configuration query string should now hold the initial planned query
            this.getMetric().setPlan(configuration.getQueryString());
            this.getMetric().setSetupTime((System.currentTimeMillis() - start));
            this.getMetric().setLifecycle(QueryMetric.Lifecycle.INITIALIZED);
            testForUncaughtException(0);
            // TODO: applyPrediction("Plan");
        } catch (Exception | Error e) {
            terminalFailure = e;
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
            removeNDC(ndcPushed);
            synchronized (nextInProgress) {
                initializing = false;
            }
            if (this.queryMetrics != null) {
                try {
                    this.queryMetrics.updateMetric(this.getMetric());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public boolean isUseResultsThread() {
        return useResultsThread;
    }

    public void setUseResultsThread(boolean useResultsThread) {
        synchronized (nextInProgress) {
            if (source != null) {
                throw new IllegalStateException("Cannot change result delivery during an execution");
            }
            this.useResultsThread = useResultsThread;
        }
    }

    /**
     * Request, rather than infer, an early return of this page. The request survives notification before the consumer starts waiting.
     */
    public void attemptForcedPageReturn() {
        synchronized (nextInProgress) {
            if (hasActiveCall() && nextInProgress.get() && currentPageCount > 0 && source != null) {
                source.requestPageReturn(currentPage);
            }
        }
    }

    /**
     * Signal the reader to stop. closeConnection also closes its resources and verifies that it has actually stopped before returning the client.
     */
    public void terminateResultsThread() {
        synchronized (nextInProgress) {
            if (source != null) {
                source.cancel();
            }
        }
    }

    public ResultsPage next() throws Exception {
        synchronized (nextInProgress) {
            if (initializing || !nextInProgress.compareAndSet(false, true)) {
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Query setup or a next call is already in progress");
            }
        }
        ResultsSource resultsSource = null;
        ResultsSource.PageRequest request = null;
        boolean ndcPushed = false;
        try {
            touch();
            long startTime = System.currentTimeMillis();
            currentThread = Thread.currentThread();
            this.logic.setPageProcessingStartTime(startTime);
            ndcPushed = addNDC();
            testForUncaughtException(0);
            checkExecution();
            List<Object> results = new ArrayList<>();
            PagePolicy policy = new PagePolicy(settings, logic, maxResults, timing);
            PageStopReason reason = finished ? PageStopReason.EXHAUSTED : null;
            if (reason == null) {
                synchronized (nextInProgress) {
                    checkExecution();
                    if (source == null) {
                        source = useResultsThread && executor != null ? new AsyncResultsSource(iter, executor) : new DirectResultsSource(iter);
                    }
                    resultsSource = source;
                    request = resultsSource.beginPage(timing == null ? Long.MAX_VALUE : timing.getPageShortCircuitTimeoutMs());
                    currentPage = request;
                }
            }
            long pageBytes = 0;
            while (reason == null) {
                checkExecution();
                resultsSource.checkFailure();
                resultsSource.flushMetrics(getMetric());
                reason = policy.beforeRead(results.size(), pageBytes, numResults, getMetric());
                if (reason != null) {
                    break;
                }
                ResultsSource.Attempt attempt;
                try {
                    attempt = resultsSource.poll(request);
                } finally {
                    resultsSource.flushMetrics(getMetric());
                }
                if (attempt.outcome == ResultsSource.Outcome.EXHAUSTED) {
                    reason = PageStopReason.EXHAUSTED;
                } else if (attempt.outcome == ResultsSource.Outcome.DEADLINE) {
                    reason = PageStopReason.DEADLINE;
                } else if (attempt.outcome == ResultsSource.Outcome.FORCED) {
                    reason = PageStopReason.FORCED;
                } else if (attempt.result instanceof EventBase && ((EventBase) attempt.result).isIntermediateResult()) {
                    reason = PageStopReason.INTERMEDIATE;
                } else {
                    results.add(attempt.result);
                    numResults++;
                    currentPageCount = results.size();
                    if (policy.countBytes()) {
                        pageBytes += ObjectSizeOf.Sizer.getObjectSize(attempt.result);
                    }
                }
                testForUncaughtException(results.size());
            }
            checkExecution();
            if (resultsSource != null) {
                resultsSource.checkFailure();
            }
            testForUncaughtException(results.size());
            ResultsPage page = resultsPage(reason, results);
            if (reason.terminal) {
                finished = true;
                terminateResultsThread();
            }
            long now = System.currentTimeMillis();
            getMetric().addPageTime(results.size(), now - startTime, startTime, now);
            if (logic instanceof BaseQueryLogic) {
                BaseQueryLogic<?> baseLogic = (BaseQueryLogic<?>) logic;
                if (baseLogic.getConfig() != null && !StringUtils.isEmpty(baseLogic.getConfig().getQueryString())) {
                    getMetric().setPlan(baseLogic.getConfig().getQueryString());
                }
            }
            lastPageNumber++;
            if (reason.lifecycle != null) {
                getMetric().setLifecycle(reason.lifecycle);
            } else if (!results.isEmpty()) {
                getMetric().setLifecycle(QueryMetric.Lifecycle.RESULTS);
            }
            return page;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            QueryException failure = new QueryException(DatawaveErrorCode.QUERY_CANCELED, e);
            terminalFailure = failure;
            canceled = true;
            getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
            getMetric().setError(failure);
            terminateResultsThread();
            throw failure;
        } catch (Exception | Error e) {
            terminalFailure = e;
            getMetric().setError(e);
            terminateResultsThread();
            throw e;
        } finally {
            synchronized (nextInProgress) {
                if (resultsSource != null) {
                    resultsSource.endPage(request);
                }
                currentPage = null;
                currentPageCount = 0;
                currentThread = null;
            }
            try {
                touch();
                removeNDC(ndcPushed);
                updateQueryMetrics();
            } finally {
                nextInProgress.set(false);
            }
        }
    }

    private void checkExecution() throws Exception {
        if (terminalFailure != null) {
            ResultsSource.rethrow(terminalFailure);
        }
        if (canceled || closed) {
            throw new QueryException(DatawaveErrorCode.QUERY_CANCELED);
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Query page retrieval interrupted");
        }
    }

    private ResultsPage resultsPage(PageStopReason reason, List<Object> results) throws QueryException {
        if (!results.isEmpty()) {
            return new ResultsPage(results, reason.status);
        }
        if (reason == PageStopReason.DEADLINE || reason == PageStopReason.INTERMEDIATE) {
            if (reason == PageStopReason.DEADLINE && !allowIntermediateEmptyPages) {
                throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT, "Query timed out waiting for a result");
            }
            // Preserve the existing execution-wide retry budget, including intermediate markers.
            currentTimeoutcount++;
            int retries = timing == null ? 0 : timing.getMaxLongRunningTimeoutRetries();
            if (retries > 0 && currentTimeoutcount >= retries) {
                throw new QueryException(DatawaveErrorCode.QUERY_TIMEOUT,
                                "Query timed out waiting for results for too many (" + currentTimeoutcount + ") cycles");
            }
            return new ResultsPage(results, ResultsPage.Status.PARTIAL);
        }
        return new ResultsPage(results, reason.terminal ? ResultsPage.Status.NONE : reason.status);
    }

    private void updateQueryMetrics() {
        if (queryMetrics != null) {
            try {
                queryMetrics.updateMetric(getMetric());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void cancel() {
        synchronized (nextInProgress) {
            canceled = true;
            getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
            terminateResultsThread();
        }
    }

    public int getCurrentPageCount() {
        return currentPageCount;
    }

    public Thread getCurrentThread() {
        return currentThread;
    }

    public RunningQueryTiming getTiming() {
        return timing;
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

    public synchronized void closeConnection(AccumuloConnectionFactory factory) throws Exception {
        ResultsSource resultsSource;
        synchronized (nextInProgress) {
            closed = true;
            resultsSource = source;
            terminateResultsThread();
        }
        Throwable failure = null;
        boolean ndcPushed = false;
        try {
            ndcPushed = addNDC();
            getMetric().setLifecycle(BaseQueryMetric.Lifecycle.CLOSED);
            if (logic != null && !logicClosed) {
                try {
                    // Closing scanners can unblock an iterator that does not respond to interruption.
                    logic.close();
                    logicClosed = true;
                } catch (Throwable t) {
                    failure = t;
                }
            }
            boolean stopped = resultsSource == null || resultsSource.awaitStopped(5000);
            if (!stopped) {
                failure = appendFailure(failure, new QueryException(DatawaveErrorCode.CLOSE_ERROR,
                                "Results reader has not stopped; retaining its client until cleanup can complete"));
            } else {
                if (!cardinalitiesWritten && iter != null && iter.getTransformer() instanceof WritesResultCardinalities) {
                    try {
                        ((WritesResultCardinalities) iter.getTransformer()).writeResultCardinalities();
                        cardinalitiesWritten = true;
                    } catch (Throwable t) {
                        failure = appendFailure(failure, t);
                    }
                }
                if (client != null) {
                    try {
                        factory.returnClient(client);
                        client = null;
                    } catch (Throwable t) {
                        failure = appendFailure(failure, t);
                    }
                }
            }
        } finally {
            removeNDC(ndcPushed);
            updateQueryMetrics();
        }
        if (failure != null) {
            ResultsSource.rethrow(failure);
        }
    }

    private static Throwable appendFailure(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
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
            ImmutablePair<Throwable,Thread> uncaughtException = handler.getUncaughtException();
            if (uncaughtException != null) {
                if (numResults > 0) {
                    log.warn("Exception with Partial Results: resultList.getResults().size() is " + numResults + ", and there was an UncaughtException:"
                                    + uncaughtException.getLeft() + " in thread " + uncaughtException.getRight());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Throwing:" + uncaughtException.getLeft() + " for query with no results");
                    }
                }
                if (uncaughtException.getLeft() instanceof QueryException) {
                    throw ((QueryException) uncaughtException.getLeft());
                }
                throw new QueryException(uncaughtException.getLeft());
            }
        }
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

}

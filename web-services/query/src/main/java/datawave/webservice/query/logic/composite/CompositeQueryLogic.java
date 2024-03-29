package datawave.webservice.query.logic.composite;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.functors.NOPTransformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import datawave.audit.SelectorExtractor;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.filtered.FilteredQueryLogic;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseResponse;

/**
 * Query Logic implementation that is configured with more than one query logic delegate. The queries are run in parallel unless configured to be sequential.
 * Results are retrieved as they come back from the delegates. This class restricts the delegates such that they have to return the same type of response
 * object. If configured to run sequentially, then the execution will terminate after the first query that returns results. Query logics will be sorted by their
 * configured name.
 */
public class CompositeQueryLogic extends BaseQueryLogic<Object> {

    private class QueryLogicHolder extends Thread {
        private GenericQueryConfiguration config;
        private String logicName;
        private QueryLogic<?> logic;
        private TransformIterator transformIterator;

        private Query settings;
        private boolean started = false;
        private long maxResults;

        public QueryLogicHolder(String logicName, QueryLogic<?> logic) {
            this.setDaemon(true);
            this.setLogicName(logicName);
            this.setLogic(logic);
            this.setName(Thread.currentThread().getName() + "-CompositeQueryLogic-" + logicName);
        }

        public boolean wasStarted() {
            return started;
        }

        public String getLogicName() {
            return logicName;
        }

        public void setLogicName(String logicName) {
            this.logicName = logicName;
        }

        public QueryLogic<?> getLogic() {
            return logic;
        }

        public void setLogic(QueryLogic<?> logic) {
            this.logic = logic;
        }

        public GenericQueryConfiguration getConfig() {
            return config;
        }

        public void setConfig(GenericQueryConfiguration config) {
            this.config = config;
        }

        public void setTransformIterator(TransformIterator transformIterator) {
            this.transformIterator = transformIterator;
        }

        public void setMaxResults(long maxResults) {
            this.maxResults = maxResults;
        }

        public long getMaxResults() {
            return maxResults;
        }

        public Query getSettings() {
            return settings;
        }

        public void setSettings(Query settings) {
            this.settings = settings;
        }

        public void run() {
            long resultCount = 0L;

            log.debug("Starting thread: " + this.getName());

            if (!started) {
                startLatch.countDown();
                started = true;
            }

            // ensure we start with a reasonable page time
            resetPageProcessingStartTime();

            // the results queue is also an exception handler
            setUncaughtExceptionHandler(results);
            boolean success = false;

            try {
                Object last = new Object();
                if (this.getMaxResults() <= 0)
                    this.setMaxResults(Long.MAX_VALUE);
                // allow us to get 1 more than maxResults so that the RunningQuery can detect the MAX_RESULTS condition.
                while ((null != last) && !interrupted && transformIterator.hasNext() && (resultCount <= this.getMaxResults())) {
                    try {
                        last = transformIterator.next();
                        if (null != last) {
                            log.debug(Thread.currentThread().getName() + ": Got result");

                            // special logic to deal with intermediate results
                            if (last instanceof EventBase && ((EventBase) last).isIntermediateResult()) {
                                // reset the page processing time to avoid getting spammed with these
                                resetPageProcessingStartTime();
                                // let the RunningQuery handle timeouts for long-running queries
                                log.debug(Thread.currentThread().getName() + ": received intermediate result");
                            } else {
                                results.add(last);
                                resultCount++;
                                log.debug(Thread.currentThread().getName() + ": Added result to queue");
                            }
                        } else {
                            log.debug(Thread.currentThread().getName() + ": Got null result");
                        }
                    } catch (InterruptedException e) {
                        // if this was on purpose, then just log and the loop will naturally exit
                        if (interrupted) {
                            log.warn("QueryLogic thread interrupted", e);
                        }
                        // otherwise something else unexpected happened
                        else {
                            throw new RuntimeException(e);
                        }
                    } catch (EmptyObjectException eoe) {
                        // ignore these
                    }
                }
                success = true;
            } catch (Exception e) {
                throw new CompositeLogicException("Failed to retrieve results", getLogicName(), e);
            } finally {
                if (success) {
                    completionLatch.countDown();
                }
                log.debug("Finished thread: " + this.getName() + " with success = " + success);
            }
        }

        public void resetPageProcessingStartTime() {
            logic.setPageProcessingStartTime(System.currentTimeMillis());
        }
    }

    protected static final Logger log = Logger.getLogger(CompositeQueryLogic.class);

    private CompositeQueryConfiguration config;

    private Map<String,QueryLogic<?>> queryLogics = null;

    private QueryLogicTransformer transformer;
    private Priority p = Priority.NORMAL;
    private volatile boolean interrupted = false;
    private volatile CountDownLatch startLatch = null;
    private volatile CountDownLatch completionLatch = null;
    private Map<String,QueryLogicHolder> logicState = new HashMap<>();
    private Map<String,QueryLogic<?>> failedQueryLogics = new HashMap<>();
    private volatile CompositeQueryLogicResults results = null;

    public CompositeQueryLogic() {}

    public CompositeQueryLogic(CompositeQueryLogic other) {
        super(other);
        this.config = CompositeQueryConfiguration.create(other);
        this.queryLogics = new TreeMap<>();
        for (Map.Entry<String,QueryLogic<?>> entry : other.getAllQueryLogics().entrySet()) {
            try {
                this.queryLogics.put(entry.getKey(), (QueryLogic) entry.getValue().clone());
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
        setPrincipal(other.getPrincipal());
    }

    public Set<Authorizations> updateRuntimeAuthorizationsAndQueryAuths(QueryLogic<?> logic, Query settings) throws AuthorizationException {
        Set<String> requestedAuths = new HashSet<>(WSAuthorizationsUtil.splitAuths(settings.getQueryAuthorizations()));

        // determine the valid authorizations for this call to be the user's auths for this logic
        DatawavePrincipal principal = (DatawavePrincipal) logic.getPrincipal();
        DatawavePrincipal queryPrincipal = principal;
        UserOperations userOperations = getUserOperations();
        if (userOperations != null) {
            principal = userOperations.getRemoteUser(principal);
        }
        logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(requestedAuths)));
        if (logic.getUserOperations() != null) {
            queryPrincipal = logic.getUserOperations().getRemoteUser(queryPrincipal);
        }

        // get the valid auths from the query user
        Collection<String> validAuths = queryPrincipal.getPrimaryUser().getAuths();
        Set<String> validRequestedAuths = new HashSet<>(requestedAuths);
        validRequestedAuths.retainAll(validAuths);
        String validQueryAuthorizations = Joiner.on(',').join(validRequestedAuths);

        // Update the set of requested auths
        settings.setQueryAuthorizations(validQueryAuthorizations);

        // recalculate the runtime query authorizations (no need to pass in userService as we have already recalculated the principal)
        Set<Authorizations> downgradedAuths = WSAuthorizationsUtil.getDowngradedAuthorizations(validQueryAuthorizations, principal, queryPrincipal);
        if (log.isTraceEnabled()) {
            log.trace("Principal auths for user " + principal.getPrimaryUser().getCommonName() + " are " + principal.getPrimaryUser().getAuths());
            log.trace("Query principal auths for " + logic.getLogicName() + " are " + validAuths);
            log.trace("Requested auths were " + requestedAuths + " of which the valid query auths are " + validQueryAuthorizations);
            log.trace("Downgraded auths are " + downgradedAuths);
        }
        return downgradedAuths;
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {

        StringBuilder logicQueryStringBuilder = new StringBuilder();
        if (!getInitializedLogics().isEmpty()) {
            logicQueryStringBuilder.append(getConfig().getQueryString());
        } else {
            logicQueryStringBuilder.append("CompositeQueryLogic: ");
        }

        Map<String,Exception> exceptions = new HashMap<>();
        if (!getUninitializedLogics().isEmpty()) {
            for (Map.Entry<String,QueryLogic<?>> next : getUninitializedLogics().entrySet()) {
                String logicName = next.getKey();
                QueryLogic<?> logic = next.getValue();
                GenericQueryConfiguration config = null;

                // start the next query logic plan expression
                if (logicQueryStringBuilder.length() > 0) {
                    logicQueryStringBuilder.append(" || ");
                }
                logicQueryStringBuilder.append("( ( logic = '").append(logicName).append("' )").append(" && ");

                try {
                    // duplicate the settings for this query
                    Query settingsCopy = settings.duplicate(settings.getQueryName() + " -> " + logicName);

                    // update the query auths and runtime query authorizations for this logic
                    runtimeQueryAuthorizations = updateRuntimeAuthorizationsAndQueryAuths(logic, settingsCopy);

                    config = logic.initialize(client, settingsCopy, runtimeQueryAuthorizations);

                    // only add this query logic to the initialized logic states if it was not simply filtered out
                    if (logic instanceof FilteredQueryLogic && ((FilteredQueryLogic) logic).isFiltered()) {
                        log.info("Dropping " + logic.getLogicName() + " as it was filtered out");
                        logicQueryStringBuilder.append("( filtered = true )");
                    } else {
                        logicQueryStringBuilder.append(config.getQueryString());

                        QueryLogicHolder holder = new QueryLogicHolder(logicName, logic);
                        holder.setConfig(config);
                        holder.setSettings(settingsCopy);
                        holder.setMaxResults(logic.getMaxResults());
                        logicState.put(logicName, holder);

                        // if doing sequential execution, then stop since we have one initialized
                        if (isShortCircuitExecution()) {
                            break;
                        }
                    }

                } catch (Exception e) {
                    exceptions.put(logicName, e);
                    log.error("Failed to initialize " + logic.getClass().getName(), e);
                    logicQueryStringBuilder.append("( failure = '").append(e.getMessage()).append("' )");
                    failedQueryLogics.put(logicName, logic);
                } finally {
                    queryLogics.remove(next.getKey());
                }

                // close out the query plan expression
                logicQueryStringBuilder.append(" )");
            }

            // if something failed initialization
            if (!exceptions.isEmpty()) {
                if (logicState.isEmpty()) {
                    // all logics have failed to initialize, rethrow the last exception caught
                    throw new CompositeLogicException("All logics have failed to initialize", exceptions);
                }

                // if all must initialize successfully, then pass up an exception
                if (isAllMustInitialize()) {
                    throw new CompositeLogicException("Failed to initialize all composite child logics", exceptions);
                }
            }

            // if results is already set, then we were merely adding a new query logic to the mix
            if (this.results == null) {
                this.results = new CompositeQueryLogicResults(this, Math.min(settings.getPagesize() * 2, 1000));
            }

            if (log.isDebugEnabled()) {
                log.debug("CompositeQuery initialized with the following queryLogics: ");
                for (Entry<String,QueryLogic<?>> entry : getInitializedLogics().entrySet()) {
                    log.debug("LogicName: " + entry.getKey() + ", tableName: " + entry.getValue().getTableName());
                }
                if (isShortCircuitExecution()) {
                    for (Entry<String,QueryLogic<?>> entry : getUninitializedLogics().entrySet()) {
                        log.debug("Pending LogicName: " + entry.getKey() + ", tableName: " + entry.getValue().getTableName());
                    }
                }
            }

            final String compositeQueryString = logicQueryStringBuilder.toString();
            CompositeQueryConfiguration config = getConfig();
            config.setQueryString(compositeQueryString);
            config.setClient(client);
            config.setQuery(settings);
            config.setAuthorizations(runtimeQueryAuthorizations);
        }
        return getConfig();
    }

    @Override
    public CompositeQueryConfiguration getConfig() {
        if (config == null) {
            config = CompositeQueryConfiguration.create();
        }

        return config;
    }

    public void setConfig(CompositeQueryConfiguration config) {
        this.config = config;
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {

        StringBuilder plans = new StringBuilder();
        int count = 1;
        String separator = Integer.toString(count++) + ": ";
        for (Map.Entry<String,QueryLogic<?>> entry : getQueryLogics().entrySet()) {
            // duplicate the settings for this query
            Query settingsCopy = settings.duplicate(settings.getQueryName() + " -> " + entry.getKey());

            // update the query auths and runtime query authorizations for this logic
            runtimeQueryAuthorizations = updateRuntimeAuthorizationsAndQueryAuths(entry.getValue(), settingsCopy);

            plans.append(separator);
            plans.append(entry.getValue().getPlan(client, settingsCopy, runtimeQueryAuthorizations, expandFields, expandValues));
            separator = "\n" + Integer.toString(count++) + ": ";
        }
        return plans.toString();
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        int count = 0;

        for (QueryLogicHolder holder : logicState.values()) {
            if (!holder.wasStarted()) {
                holder.getLogic().setupQuery(holder.getConfig());
                TransformIterator transformIterator = holder.getLogic().getTransformIterator(holder.getSettings());
                holder.setTransformIterator(transformIterator);
                count++;
            }
        }

        startLatch = new CountDownLatch(count);
        completionLatch = new CountDownLatch(count);

        for (QueryLogicHolder holder : logicState.values()) {
            if (!holder.wasStarted()) {
                holder.start();
            }
        }

        // Wait until all threads have started
        startLatch.await();
        log.trace("All threads have started.");
    }

    @Override
    public Priority getConnectionPriority() {
        return p;
    }

    public void setConnectionPriority(String priority) {
        p = Priority.valueOf(priority);
    }

    /**
     * Method used to check that the configuration is correct and to get the response class by QueryExecutorBean.listQueryLogic()
     */
    @Override
    public synchronized QueryLogicTransformer getTransformer(Query settings) {
        ResultsPage emptyList = new ResultsPage();
        Class<? extends BaseResponse> responseClass = null;
        List<QueryLogicTransformer> delegates = new ArrayList<>();
        for (QueryLogic logic : getQueryLogics().values()) {
            QueryLogicTransformer t = logic.getTransformer(settings);
            delegates.add(t);
            BaseResponse refResponse = t.createResponse(emptyList);
            if (null == responseClass) {
                responseClass = refResponse.getClass();
            } else {
                if (!responseClass.equals(refResponse.getClass())) {
                    throw new RuntimeException("All query logics must use transformers that return the same object type");
                }
            }
        }
        this.transformer = new CompositeQueryLogicTransformer(delegates);
        return this.transformer;
    }

    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        // The objects put into the pageQueue have already been transformed.
        // We will iterate over the pagequeue with the No-Op transformer
        return new TransformIterator(results.iterator(), NOPTransformer.nopTransformer());
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new CompositeQueryLogic(this);
    }

    @Override
    public void close() {
        this.interrupted = true;
        for (QueryLogicHolder holder : logicState.values()) {
            holder.getLogic().close();
            holder.interrupt();
        }
        for (QueryLogicHolder holder : logicState.values()) {
            try {
                holder.join();
            } catch (InterruptedException e) {
                log.error("Error joining query logic thread", e);
                throw new RuntimeException("Error joining query logic thread", e);
            }
        }
        logicState.clear();
        if (null != results)
            results.clear();
    }

    public Map<String,QueryLogic<?>> getQueryLogics() {
        TreeMap<String,QueryLogic<?>> logics = new TreeMap<>();
        logics.putAll(getUninitializedLogics());
        logics.putAll(getInitializedLogics());
        return logics;
    }

    public Map<String,QueryLogic<?>> getAllQueryLogics() {
        TreeMap<String,QueryLogic<?>> logics = new TreeMap<>();
        logics.putAll(getUninitializedLogics());
        logics.putAll(getInitializedLogics());
        logics.putAll(getFailedLogics());
        return logics;
    }

    public Map<String,QueryLogic<?>> getFailedLogics() {
        if (failedQueryLogics != null) {
            return failedQueryLogics;
        } else {
            return new HashMap<>();
        }
    }

    public Map<String,QueryLogic<?>> getUninitializedLogics() {
        if (queryLogics != null) {
            return new TreeMap<>(queryLogics);
        } else {
            return new TreeMap<>();
        }
    }

    public Map<String,QueryLogic<?>> getInitializedLogics() {
        TreeMap<String,QueryLogic<?>> logics = new TreeMap<>();
        if (logicState != null) {
            logicState.entrySet().forEach(e -> logics.put(e.getKey(), e.getValue().getLogic()));
        }
        return logics;
    }

    public void setQueryLogics(Map<String,QueryLogic<?>> queryLogics) {
        this.queryLogics = new TreeMap<>(queryLogics);
    }

    @Override
    public void preInitialize(Query settings, Set<Authorizations> queryAuths) {
        for (QueryLogic logic : getUninitializedLogics().values()) {
            logic.preInitialize(settings, queryAuths);
        }
    }

    public UserOperations getUserOperations() {
        // if any of the underlying logics have a non-null user operations, then
        // we need to return an instance that combines auths across the underlying
        // query logics
        boolean includeLocal = false;
        List<UserOperations> userOperations = new ArrayList<>();
        for (QueryLogic<?> logic : getQueryLogics().values()) {
            UserOperations ops = logic.getUserOperations();
            if (ops == null) {
                includeLocal = true;
            } else {
                userOperations.add(ops);
            }
        }
        if (!userOperations.isEmpty()) {
            return new CompositeUserOperations(userOperations, includeLocal, isShortCircuitExecution(), responseObjectFactory);
        }
        return null;
    }

    @Override
    public boolean canRunQuery(Principal principal) {
        // user can run this composite query if they can run at least one of the configured query logics
        for (Map.Entry<String,QueryLogic<?>> entry : getUninitializedLogics().entrySet()) {
            if (!entry.getValue().canRunQuery(principal)) {
                queryLogics.remove(entry.getKey());
            }
        }
        return (!getUninitializedLogics().isEmpty());
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        // Create a UNION set. Should it be an intersection?
        for (QueryLogic<?> l : getQueryLogics().values()) {
            params.addAll(l.getOptionalQueryParameters());
        }
        return params;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> params = new TreeSet<>();
        for (QueryLogic<?> l : getQueryLogics().values()) {
            params.addAll(l.getRequiredQueryParameters());
        }
        return params;
    }

    @Override
    public Set<String> getExampleQueries() {
        Set<String> params = new TreeSet<>();
        for (QueryLogic<?> l : getQueryLogics().values()) {
            Set<String> examples = l.getExampleQueries();
            if (examples != null) {
                params.addAll(examples);
            }
        }
        return params.isEmpty() ? null : params;
    }

    /**
     * We can run the query if we can and at least of one of the children can.
     *
     * @return canRunQuery
     */
    @Override
    public boolean canRunQuery() {
        if (super.canRunQuery()) {
            for (QueryLogic<?> logic : getQueryLogics().values()) {
                if (logic.canRunQuery()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The selector extractor is dependent on the children. Return the first non-null instance.
     *
     * @return selector extractor
     */
    @Override
    public SelectorExtractor getSelectorExtractor() {
        for (QueryLogic<?> logic : getQueryLogics().values()) {
            SelectorExtractor extractor = logic.getSelectorExtractor();
            if (extractor != null) {
                return extractor;
            }
        }
        return null;
    }

    /**
     * Setting the principal is called after the logic is created. Pass this on to the children.
     *
     * @param principal
     *            the principal
     */
    @Override
    public void setPrincipal(Principal principal) {
        super.setPrincipal(principal);
        for (QueryLogic<?> logic : getQueryLogics().values()) {
            logic.setPrincipal(principal);
        }
    }

    /**
     * Setting the page processing start time is called after the logic is created. Pass this on to the children.
     *
     * @param pageProcessingStartTime
     *            the processing start time
     */
    @Override
    public void setPageProcessingStartTime(long pageProcessingStartTime) {
        super.setPageProcessingStartTime(pageProcessingStartTime);
        for (QueryLogic<?> logic : getQueryLogics().values()) {
            logic.setPageProcessingStartTime(pageProcessingStartTime);
        }
    }

    @Override
    public boolean isLongRunningQuery() {
        for (QueryLogic<?> l : getQueryLogics().values()) {
            if (l.isLongRunningQuery()) {
                return true;
            }
        }
        return false;
    }

    public boolean isAllMustInitialize() {
        return getConfig().isAllMustInitialize();
    }

    public void setAllMustInitialize(boolean allMustInitialize) {
        getConfig().setAllMustInitialize(allMustInitialize);
    }

    public boolean isShortCircuitExecution() {
        return getConfig().isShortCircuitExecution();
    }

    public void setShortCircuitExecution(boolean shortCircuit) {
        getConfig().setShortCircuitExecution(shortCircuit);
    }

    public Query getSettings() {
        return getConfig().getQuery();
    }

    public void setSettings(Query settings) {
        getConfig().setQuery(settings);
    }

    public CountDownLatch getStartLatch() {
        return startLatch;
    }

    public CountDownLatch getCompletionLatch() {
        return completionLatch;
    }
}

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
import datawave.security.util.AuthorizationsUtil;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.result.BaseResponse;

/**
 * Query Logic implementation that is configured with more than one query logic delegate. The queries are run in parallel and results are retrieved as they come
 * back from the delegates. This class restricts the delegates such that they have to return the same type of response object and two query logics with the same
 * class name and tableName cannot be configured.
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

            log.trace("Starting thread: " + this.getName());

            if (!started) {
                startLatch.countDown();
                started = true;
            }

            // the results queue is also an exception handler
            setUncaughtExceptionHandler(results);
            boolean success = false;

            try {
                Object last = new Object();
                if (this.getMaxResults() < 0)
                    this.setMaxResults(Long.MAX_VALUE);
                while ((null != last) && !interrupted && transformIterator.hasNext() && (resultCount < this.getMaxResults())) {
                    try {
                        last = transformIterator.next();
                        if (null != last) {
                            log.debug(Thread.currentThread().getName() + ": Added object to results");
                            results.add(last);
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
                    }
                    resultCount++;
                }
                success = true;
            } catch (Exception e) {
                throw new CompositeLogicException("Failed to retrieve results", Collections.singletonMap(getLogicName(), e));
            } finally {
                if (success) {
                    completionLatch.countDown();
                }
                log.trace("Finished thread: " + this.getName() + " with success = " + success);
            }
        }

    }

    protected static final Logger log = Logger.getLogger(CompositeQueryLogic.class);

    private Map<String,QueryLogic<?>> queryLogics = null;

    // Specified whether all queries must succeed initialization
    private boolean allMustInitialize = false;

    private QueryLogicTransformer transformer;
    private Priority p = Priority.NORMAL;
    private volatile boolean interrupted = false;
    private volatile CountDownLatch startLatch = null;
    private volatile CountDownLatch completionLatch = null;
    private Map<String,QueryLogicHolder> logicState = new HashMap<>();
    private volatile CompositeQueryLogicResults results = null;

    public CompositeQueryLogic() {}

    public CompositeQueryLogic(CompositeQueryLogic other) {
        super(other);
        if (other.queryLogics != null) {
            this.queryLogics = new HashMap<>();
            for (Map.Entry<String,QueryLogic<?>> entry : other.queryLogics.entrySet()) {
                try {
                    this.queryLogics.put(entry.getKey(), (QueryLogic) entry.getValue().clone());
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
            }
            setPrincipal(other.getPrincipal());
        }
        this.allMustInitialize = other.allMustInitialize;
    }

    public Set<Authorizations> updateRuntimeAuthorizationsAndQueryAuths(QueryLogic<?> logic, Query settings) throws AuthorizationException {
        Set<String> requestedAuths = new HashSet<>(AuthorizationsUtil.splitAuths(settings.getQueryAuthorizations()));

        // determine the valid authorizations for this call to be the user's auths for this logic
        DatawavePrincipal principal = (DatawavePrincipal) logic.getPrincipal();
        DatawavePrincipal queryPrincipal = principal;
        UserOperations userOperations = getUserOperations();
        if (userOperations != null) {
            principal = userOperations.getRemoteUser(principal);
        }
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
        return AuthorizationsUtil.getDowngradedAuthorizations(validQueryAuthorizations, principal, queryPrincipal);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        StringBuilder logicQueryStringBuilder = new StringBuilder("CompositeQueryLogic: ");
        Map<String,Exception> exceptions = new HashMap<>();
        if (this.queryLogics != null) {
            Iterator<Map.Entry<String,QueryLogic<?>>> itr = this.queryLogics.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<String,QueryLogic<?>> next = itr.next();
                String logicName = next.getKey();
                QueryLogic<?> logic = next.getValue();
                GenericQueryConfiguration config = null;
                try {
                    // duplicate the settings for this query
                    Query settingsCopy = settings.duplicate(settings.getQueryName() + " -> " + logicName);

                    // update the query auths and runtime query authorizations for this logic
                    runtimeQueryAuthorizations = updateRuntimeAuthorizationsAndQueryAuths(logic, settingsCopy);

                    config = logic.initialize(client, settingsCopy, runtimeQueryAuthorizations);
                    if (logicQueryStringBuilder.length() > 0) {
                        logicQueryStringBuilder.append(" || ");
                    }
                    logicQueryStringBuilder.append("( ( logic = '").append(logic.getLogicName()).append("' )");
                    logicQueryStringBuilder.append(" && ").append(config.getQueryString()).append(" )");
                    QueryLogicHolder holder = new QueryLogicHolder(logicName, logic);
                    holder.setConfig(config);
                    holder.setSettings(settingsCopy);
                    holder.setMaxResults(logic.getMaxResults());
                    logicState.put(logicName, holder);
                } catch (Exception e) {
                    exceptions.put(logicName, e);
                    log.error("Failed to initialize " + logic.getClass().getName(), e);
                    itr.remove();
                }
            }
        }

        if (!exceptions.isEmpty()) {
            if (logicState.isEmpty()) {
                // all logics have failed to initialize, rethrow the last exception caught
                throw new CompositeLogicException("All logics have failed to initialize", exceptions);
            }

            // if all must initialize successfully, then pass up an exception
            if (allMustInitialize) {
                throw new CompositeLogicException("Failed to initialize all composite child logics", exceptions);
            }
        }

        startLatch = new CountDownLatch(logicState.size());
        completionLatch = new CountDownLatch(logicState.size());
        this.results = new CompositeQueryLogicResults(Math.min(settings.getPagesize() * 2, 1000), completionLatch);
        if (log.isDebugEnabled()) {
            log.debug("CompositeQuery initialized with the following queryLogics: ");
            for (Entry<String,QueryLogicHolder> entry : this.logicState.entrySet()) {
                log.debug("\tLogicName: " + entry.getKey() + ", tableName: " + entry.getValue().getLogic().getTableName());
            }
        }

        final String compositeQueryString = logicQueryStringBuilder.toString();
        return new GenericQueryConfiguration() {
            @Override
            public String getQueryString() {
                return compositeQueryString;
            }
        };
    }

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {

        StringBuilder plans = new StringBuilder();
        int count = 1;
        String separator = Integer.toString(count++) + ": ";
        for (Map.Entry<String,QueryLogic<?>> entry : queryLogics.entrySet()) {
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
        for (QueryLogicHolder holder : logicState.values()) {
            holder.getLogic().setupQuery(holder.getConfig());
            TransformIterator transformIterator = holder.getLogic().getTransformIterator(holder.getSettings());
            holder.setTransformIterator(transformIterator);
        }
        for (QueryLogicHolder holder : logicState.values()) {
            holder.start();
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
    public synchronized QueryLogicTransformer getTransformer(Query settings) throws QueryException {
        ResultsPage emptyList = new ResultsPage();
        Class<? extends BaseResponse> responseClass = null;
        List<QueryLogicTransformer> delegates = new ArrayList<>();
        if (this.queryLogics != null) {
            for (QueryLogic<?> logic : this.queryLogics.values()) {
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
        return this.queryLogics;
    }

    public void setQueryLogics(Map<String,QueryLogic<?>> queryLogics) {
        this.queryLogics = queryLogics;
    }

    public UserOperations getUserOperations() {
        // if any of the underlying logics have a non-null user operations, then
        // we need to return an instance that combines auths across the underlying
        // query logics
        boolean includeLocal = false;
        List<UserOperations> userOperations = new ArrayList<>();
        for (QueryLogic<?> logic : this.queryLogics.values()) {
            UserOperations ops = logic.getUserOperations();
            if (ops == null) {
                includeLocal = true;
            } else {
                userOperations.add(ops);
            }
        }
        if (!userOperations.isEmpty()) {
            return new CompositeUserOperations(userOperations, includeLocal, responseObjectFactory);
        }
        return null;
    }

    @Override
    public boolean canRunQuery(Principal principal) {
        if (this.queryLogics == null) {
            return false;
        }

        // user can run this composite query if they can run at least one of the configured query logics
        Iterator<QueryLogic<?>> itr = this.queryLogics.values().iterator();
        while (itr.hasNext()) {
            QueryLogic<?> logic = itr.next();
            if (!logic.canRunQuery(principal)) {
                itr.remove();
            }
        }
        return (!this.queryLogics.isEmpty());
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        // Create a UNION set. Should it be an intersection?
        if (this.queryLogics != null) {
            for (QueryLogic<?> l : this.queryLogics.values()) {
                params.addAll(l.getOptionalQueryParameters());
            }
        }
        return params;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> params = new TreeSet<>();
        if (this.queryLogics != null) {
            for (QueryLogic<?> l : this.queryLogics.values()) {
                params.addAll(l.getRequiredQueryParameters());
            }
        }
        return params;
    }

    @Override
    public Set<String> getExampleQueries() {
        Set<String> params = new TreeSet<>();
        if (this.queryLogics != null) {
            for (QueryLogic<?> l : this.queryLogics.values()) {
                Set<String> examples = l.getExampleQueries();
                if (examples != null) {
                    params.addAll(examples);
                }
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
            if (this.queryLogics != null) {
                for (QueryLogic<?> logic : this.queryLogics.values()) {
                    if (logic.canRunQuery()) {
                        return true;
                    }
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
        if (this.queryLogics != null) {
            for (QueryLogic<?> logic : this.queryLogics.values()) {
                SelectorExtractor extractor = logic.getSelectorExtractor();
                if (extractor != null) {
                    return extractor;
                }
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
        if (this.queryLogics != null) {
            for (QueryLogic<?> logic : this.queryLogics.values()) {
                logic.setPrincipal(principal);
            }
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
        if (this.queryLogics != null) {
            for (QueryLogic<?> logic : this.queryLogics.values()) {
                logic.setPageProcessingStartTime(pageProcessingStartTime);
            }
        }
    }

    public boolean isAllMustInitialize() {
        return allMustInitialize;
    }

    public void setAllMustInitialize(boolean allMustInitialize) {
        this.allMustInitialize = allMustInitialize;
    }
}

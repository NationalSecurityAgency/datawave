package datawave.webservice.query.logic.composite;

import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.result.BaseResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.functors.NOPTransformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import javax.enterprise.inject.Typed;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Query Logic implementation that is configured with more than one query logic delegate. The queries are run in parallel and results are retrieved as they come
 * back from the delegates. This class restricts the delegates such that they have to return the same type of response object and two query logics with the same
 * class name and tableName cannot be configured.
 */
public class CompositeQueryLogic extends BaseQueryLogic<Object> {
    
    @Typed
    public static class QueryLogicComparator implements Comparator<QueryLogic<?>> {
        @Override
        public int compare(QueryLogic<?> o1, QueryLogic<?> o2) {
            int result = (o1.getClass().getName().compareTo(o2.getClass().getName()));
            if (result == 0) {
                return (o1.getTableName().compareTo(o2.getTableName()));
            } else {
                return result;
            }
        }
    }
    
    private class QueryLogicHolder extends Thread {
        private GenericQueryConfiguration config;
        private TransformIterator transformIterator;
        private Query settings;
        private boolean started = false;
        private long maxResults;
        
        public QueryLogicHolder(String logicName) {
            this.setDaemon(true);
            this.setName(Thread.currentThread().getName() + "-CompositeQueryLogic-" + logicName + "-" + UUID.randomUUID());
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
                        log.warn("QueryLogic thread interrupted", e);
                    }
                    resultCount++;
                }
                
            } finally {
                completionLatch.countDown();
                log.trace("Finished thread: " + this.getName());
            }
        }
        
    }
    
    protected static final Logger log = Logger.getLogger(CompositeQueryLogic.class);
    
    private List<QueryLogic<?>> queryLogics = null;
    private QueryLogicTransformer transformer;
    private Priority p = Priority.NORMAL;
    private volatile boolean interrupted = false;
    private CountDownLatch startLatch = null;
    private CountDownLatch completionLatch = null;
    private Map<QueryLogic<?>,QueryLogicHolder> logicState = new TreeMap<>(new QueryLogicComparator());
    private CompositeQueryLogicResults results = null;
    
    public CompositeQueryLogic() {}
    
    public CompositeQueryLogic(CompositeQueryLogic other) {
        super(other);
        this.queryLogics = new ArrayList<>(other.queryLogics);
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        
        for (QueryLogic<?> logic : queryLogics) {
            final QueryLogic<?> queryLogic = logic;
            int count = CollectionUtils.countMatches(
                            queryLogics,
                            object -> {
                                if (object instanceof QueryLogic<?>) {
                                    if (queryLogic.getClass().equals(((QueryLogic<?>) object).getClass())
                                                    && queryLogic.getTableName().equals(((QueryLogic<?>) object).getTableName())) {
                                        return true;
                                    } else {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            });
            
            if (count > 1) {
                throw new RuntimeException("More than one instance of query logic class configured with the same table: " + logic.getClass().getName());
            }
        }
        
        Iterator<QueryLogic<?>> itr = queryLogics.iterator();
        StringBuilder logicQueryStringBuilder = new StringBuilder("CompositeQueryLogic: ");
        while (itr.hasNext()) {
            QueryLogic<?> logic = itr.next();
            GenericQueryConfiguration config = null;
            try {
                config = logic.initialize(connection, settings, runtimeQueryAuthorizations);
                logicQueryStringBuilder.append("(table=" + config.getTableName());
                logicQueryStringBuilder.append(",query=" + config.getQueryString());
                logicQueryStringBuilder.append(") ");
                QueryLogicHolder holder = new QueryLogicHolder(logic.getClass().getSimpleName());
                holder.setConfig(config);
                holder.setSettings(settings);
                holder.setMaxResults(logic.getMaxResults());
                logicState.put(logic, holder);
            } catch (Exception e) {
                log.info(e.getMessage() + " removing query logic " + logic.getClass().getName() + " from CompositeQuery");
                itr.remove();
                if (itr.hasNext() == false && logicState.isEmpty()) {
                    // all logics have failed to initialize, rethrow the last exception caught
                    throw new IllegalStateException("All logics have failed to initialize", e);
                }
            }
        }
        startLatch = new CountDownLatch(logicState.values().size());
        completionLatch = new CountDownLatch(logicState.values().size());
        this.results = new CompositeQueryLogicResults(Math.min(settings.getPagesize() * 2, 1000), completionLatch);
        if (log.isDebugEnabled()) {
            log.debug("CompositeQuery initialized with the following queryLogics: ");
            for (Entry<QueryLogic<?>,QueryLogicHolder> entry : this.logicState.entrySet()) {
                log.debug("\tLogicName: " + entry.getKey().getClass().getSimpleName() + ", tableName: " + entry.getKey().getTableName());
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
    public String getPlan(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        
        StringBuilder plans = new StringBuilder();
        int count = 1;
        String separator = Integer.toString(count++) + ": ";
        for (Entry<QueryLogic<?>,QueryLogicHolder> entry : logicState.entrySet()) {
            plans.append(separator);
            plans.append(entry.getKey().getPlan(connection, settings, runtimeQueryAuthorizations, expandFields, expandValues));
            separator = "\n" + Integer.toString(count++) + ": ";
        }
        return plans.toString();
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        for (Entry<QueryLogic<?>,QueryLogicHolder> entry : logicState.entrySet()) {
            entry.getKey().setupQuery(entry.getValue().getConfig());
            TransformIterator transformIterator = entry.getKey().getTransformIterator(entry.getValue().getSettings());
            entry.getValue().setTransformIterator(transformIterator);
        }
        for (Entry<QueryLogic<?>,QueryLogicHolder> entry : logicState.entrySet()) {
            entry.getValue().start();
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
        for (QueryLogic<?> logic : queryLogics) {
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
        if (null == this.transformer) {
            this.transformer = new CompositeQueryLogicTransformer(delegates);
        }
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
        for (Entry<QueryLogic<?>,QueryLogicHolder> entry : logicState.entrySet()) {
            entry.getKey().close();
            entry.getValue().interrupt();
        }
        for (Entry<QueryLogic<?>,QueryLogicHolder> entry : logicState.entrySet()) {
            try {
                entry.getValue().join();
            } catch (InterruptedException e) {
                log.error("Error joining query logic thread", e);
                throw new RuntimeException("Error joining query logic thread", e);
            }
        }
        logicState.clear();
        if (null != results)
            results.clear();
    }
    
    public List<QueryLogic<?>> getQueryLogics() {
        return queryLogics;
    }
    
    public void setQueryLogics(List<QueryLogic<?>> queryLogics) {
        this.queryLogics = queryLogics;
    }
    
    @Override
    public boolean canRunQuery(Principal principal) {
        // user can run this composite query if they can run at least one of the configured query logics
        Iterator<QueryLogic<?>> itr = queryLogics.iterator();
        while (itr.hasNext()) {
            QueryLogic<?> logic = itr.next();
            if (!logic.canRunQuery(principal)) {
                itr.remove();
            }
        }
        return (!queryLogics.isEmpty());
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        // Create a UNION set. Should it be an intersection?
        for (QueryLogic<?> l : this.queryLogics) {
            params.addAll(l.getOptionalQueryParameters());
        }
        return params;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> params = new TreeSet<>();
        for (QueryLogic<?> l : this.queryLogics) {
            params.addAll(l.getRequiredQueryParameters());
        }
        return params;
    }
    
    @Override
    public Set<String> getExampleQueries() {
        Set<String> params = new TreeSet<>();
        for (QueryLogic<?> l : this.queryLogics) {
            Set<String> examples = l.getExampleQueries();
            if (examples != null) {
                params.addAll(examples);
            }
        }
        return params.isEmpty() ? null : params;
    }
}

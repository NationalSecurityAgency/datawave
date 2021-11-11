package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.Callable;

public class FindWorkTask implements Callable<Void> {
    private Logger log = Logger.getLogger(FindWorkTask.class);
    protected final QueryStorageCache cache;
    protected final QueryExecutor executor;
    // a cache of the last N close cancel queries that we handled to avoid excessive executor executions and subsequent query cache hits
    protected final CloseCancelCache closeCancelCache;
    
    private final String originService;
    private final String destinationService;
    
    public FindWorkTask(QueryStorageCache cache, QueryExecutor executor, CloseCancelCache closeCancelCache, String originService, String destinationService) {
        this.cache = cache;
        this.executor = executor;
        this.closeCancelCache = closeCancelCache;
        this.originService = originService;
        this.destinationService = destinationService;
    }
    
    @Override
    public Void call() throws Exception {
        for (QueryStatus queryStatus : cache.getQueryStatus()) {
            String queryId = queryStatus.getQueryKey().getQueryId();
            switch (queryStatus.getQueryState()) {
                case CLOSED:
                    if (closeCancelCache.add(queryId)) {
                        log.debug("Closing " + queryId);
                        executor.handleRemoteRequest(QueryRequest.close(queryId), originService, destinationService);
                    }
                    break;
                case CANCELED:
                    if (closeCancelCache.add(queryId)) {
                        log.debug("Cancelling " + queryId);
                        executor.handleRemoteRequest(QueryRequest.cancel(queryId), originService, destinationService);
                    }
                    break;
                case CREATED:
                    log.debug("Nexting " + queryId);
                    // TODO: The monitor task lease is 100ms by default. Is that enough time to ensure that the handleRemoteRequest call will finish?
                    // Should we create a new thread to handle the remote request instead so that we can return immediately?
                    // even if the next task is to plan, this will take care of it
                    executor.handleRemoteRequest(QueryRequest.next(queryId), originService, destinationService);
                    break;
                case DEFINED:
                case FAILED:
                    // noop
                    break;
            }
        }
        return null;
    }
    
    /**
     * A class that provides quick containership test via a hashset and keeps a max size by aging off the oldest elements as new ones are added
     */
    public static class CloseCancelCache extends HashSet<String> {
        private final CircularFifoQueue<String> list;
        
        public CloseCancelCache(int maxSize) {
            list = new CircularFifoQueue<>(maxSize);
        }
        
        @Override
        public boolean add(String t) {
            if (super.add(t)) {
                if (list.size() == list.maxSize()) {
                    super.remove(list.remove());
                }
                list.add(t);
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private final Iterator<String> iterator = list.iterator();
                private String lastReturned = null;
                
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
                
                @Override
                public String next() {
                    return iterator.next();
                }
                
                @Override
                public void remove() {
                    if (lastReturned == null) {
                        throw new IllegalStateException();
                    }
                    iterator.remove();
                    CloseCancelCache.this.superRemove(lastReturned);
                }
            };
        }
        
        private boolean superRemove(Object o) {
            return super.remove(o);
        }
        
        @Override
        public boolean remove(Object o) {
            if (superRemove(o)) {
                list.remove(o);
                return true;
            }
            return false;
        }
        
        @Override
        public void clear() {
            super.clear();
            list.clear();
        }
        
        @Override
        public Object clone() {
            CloseCancelCache set = new CloseCancelCache(list.maxSize());
            set.addAll(this);
            return set;
        }
        
        @Override
        public Spliterator<String> spliterator() {
            return list.spliterator();
        }
    }
}

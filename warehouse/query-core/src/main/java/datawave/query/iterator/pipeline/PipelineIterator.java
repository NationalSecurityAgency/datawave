package datawave.query.iterator.pipeline;

import datawave.core.iterators.IteratorThreadPoolManager;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.NestedQuery;
import datawave.query.iterator.NestedQueryIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the iterator that handles the evaluation pipelines. Essentially it will queue up N evaluations. On each hasNext and next call, it will pull the
 * results ready from the top and cache the non-null results in a results queue.
 */
public class PipelineIterator implements Iterator<Entry<Key,Document>> {
    
    private static final Logger log = Logger.getLogger(PipelineIterator.class);
    protected final YieldCallback<Key> yield;
    protected final long yieldThresholdMs;
    protected final NestedIterator<Key> docSource;
    protected final PipelinePool pipelines;
    protected final Queue<Tuple2<Future<?>,Pipeline>> evaluationQueue;
    protected Key lastKeyEvaluated = null;
    protected final Queue<Entry<Key,Document>> results;
    protected final int maxResults;
    protected final QuerySpanCollector querySpanCollector;
    protected final QuerySpan querySpan;
    protected boolean collectTimingDetails = false;
    protected IteratorEnvironment env;
    protected Collection<ByteSequence> columnFamilies;
    protected boolean inclusive;
    
    public PipelineIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        this.docSource = documents;
        this.pipelines = new PipelinePool(maxPipelines, querySpanCollector, sourceIterator, sourceForDeepCopy, env);
        this.evaluationQueue = new LinkedList<>();
        this.results = new LinkedList<>();
        this.maxResults = maxCachedResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.env = env;
        this.yield = yieldCallback;
        this.yieldThresholdMs = yieldThresholdMs;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
    }
    
    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        // if we had already yielded, then leave gracefully
        if (yield != null && yield.hasYielded()) {
            return false;
        }
        
        Entry<Key,Document> next = getNext(false);
        if (log.isTraceEnabled()) {
            log.trace("QueryIterator.hasNext() -> " + (next == null ? null : next.getKey()));
        }
        return (next != null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Entry<Key,Document> next() {
        // if we had already yielded, then leave gracefully
        if (yield != null && yield.hasYielded()) {
            return null;
        }
        
        Entry<Key,Document> next = getNext(true);
        if (log.isTraceEnabled()) {
            log.trace("QueryIterator.next() -> " + (next == null ? null : next.getKey()));
        }
        return next;
    }
    
    /**
     * Get the next non-null result from the queue. Pop/remove that pipeline as specified.
     * 
     * @param remove
     * @return the next non-null entry. null if there are no more entries to get.
     */
    private Entry<Key,Document> getNext(boolean remove) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("getNext(" + remove + ") start: " + evaluationQueue.size() + " cached: " + results.size());
            }
            
            // cache the next non-null result if we do not already have one
            if (results.isEmpty()) {
                cacheNextResult();
            }
            
            if (log.isTraceEnabled()) {
                log.trace("getNext(" + remove + ") cache: " + evaluationQueue.size() + " cached: " + results.size());
            }
            
            // flush any completed results to the results queue
            flushCompletedResults();
            
            if (log.isTraceEnabled()) {
                log.trace("getNext(" + remove + ") flush: " + evaluationQueue.size() + " cached: " + results.size());
            }
            
            // get/remove and return the next result, null if we are done
            Entry<Key,Document> next = null;
            if (!results.isEmpty()) {
                if (remove) {
                    next = results.poll();
                } else {
                    next = results.peek();
                }
            }
            
            return next;
        } catch (Exception e) {
            // cancel out existing executions
            cancel();
            
            // if we yielded, then leave gracefully
            if (yield != null && yield.hasYielded()) {
                return null;
            }
            
            log.error("Failed to retrieve evaluation pipeline result", e);
            throw new RuntimeException("Failed to retrieve evaluation pipeline result", e);
        }
    }
    
    /**
     * poll results from the evaluation queue until we get one that is non-null
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void cacheNextResult() throws InterruptedException, ExecutionException {
        Entry<Key,Document> result = null;
        
        long startMs = System.currentTimeMillis();
        while (!evaluationQueue.isEmpty() && result == null) {
            // we must have at least evaluated one thing in order to yield, otherwise we will have not progressed at all
            if (yield != null && lastKeyEvaluated != null) {
                long delta = System.currentTimeMillis() - startMs;
                if (delta > yieldThresholdMs) {
                    yield.yield(lastKeyEvaluated);
                    if (log.isDebugEnabled())
                        log.debug("Yielding at " + lastKeyEvaluated);
                    throw new IterationInterruptedException("Yielding at " + lastKeyEvaluated);
                }
                try {
                    result = poll(yieldThresholdMs - delta);
                } catch (TimeoutException e) {
                    yield.yield(lastKeyEvaluated);
                    if (log.isDebugEnabled())
                        log.debug("Yielding at " + lastKeyEvaluated);
                    throw new IterationInterruptedException("Yielding at " + lastKeyEvaluated);
                }
            } else {
                try {
                    result = poll(Long.MAX_VALUE);
                } catch (TimeoutException e) {
                    // should be impossible with a Long.MAX_VALUE, but we can wait another 292 million years
                    log.error("We have been waiting for 292 million years, trying again");
                }
            }
        }
    }
    
    /**
     * flush the results from the evaluation queue that are complete up to the max number of cached results
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void flushCompletedResults() throws InterruptedException, ExecutionException {
        while (!evaluationQueue.isEmpty() && evaluationQueue.peek().first().isDone() && results.size() < this.maxResults) {
            try {
                poll(Long.MAX_VALUE);
            } catch (TimeoutException e) {
                // should be impossible with a Long.MAX_VALUE, but we can wait another 292 million years
                log.error("We have been waiting for 292 million years, trying again");
            }
        }
    }
    
    /**
     * Poll the next evaluation future, start a new evaluation in its place, queue and return the result. This assumes there is a queued evaluation to get.
     * 
     * @return The next evaluation result
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Entry<Key,Document> poll(long waitMs) throws InterruptedException, ExecutionException, TimeoutException {
        // get the next evaluated result
        Tuple2<Future<?>,Pipeline> nextFuture = evaluationQueue.poll();
        
        Entry<Key,Document> result = null;
        try {
            if (log.isTraceEnabled()) {
                Key docKey = nextFuture.second().getSource().getKey();
                log.trace("Polling for result from " + docKey);
            }
            
            // wait for it to complete if not already done
            if (!nextFuture.first().isDone()) {
                long start = System.currentTimeMillis();
                
                nextFuture.first().get(waitMs, TimeUnit.MILLISECONDS);
                
                if (log.isDebugEnabled()) {
                    long wait = System.currentTimeMillis() - start;
                    log.debug("Waited " + wait + "ms for the top evaluation in a queue of " + evaluationQueue.size() + " pipelines");
                }
            }
            
            // call get to ensure that we throw any exception that occurred
            nextFuture.first().get();
            
            // pull the result
            result = nextFuture.second().getResult();
            
            if (log.isTraceEnabled()) {
                Key docKey = nextFuture.second().getSource().getKey();
                log.trace("Polling for result from " + docKey + " was " + (result == null ? "empty" : "successful"));
            }
            
            // record the last evaluated key
            lastKeyEvaluated = nextFuture.second().getSource().getKey();
        } catch (Exception e) {
            Key docKey = nextFuture.second().getSource().getKey();
            log.error("Failed polling for result from " + docKey + "; cancelling remaining evaluations and flushing results", e);
            cancel();
            throw e;
        } finally {
            // return the pipeline for reuse
            pipelines.checkIn(nextFuture.second());
        }
        
        // start a new evaluation if we can
        if (docSource.hasNext()) {
            Key keySource = docSource.next();
            NestedQuery<Key> nestedQuery = null;
            if (docSource instanceof NestedQueryIterator) {
                nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
            }
            
            evaluate(keySource, docSource.document(), nestedQuery, columnFamilies, inclusive);
            if (collectTimingDetails) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
        
        // put the result into the queue if non-null
        if (result != null) {
            results.add(result);
        }
        
        return result;
    }
    
    /**
     * Cancel all of the queued evaluations
     */
    private void cancel() {
        while (!evaluationQueue.isEmpty()) {
            Tuple2<Future<?>,Pipeline> nextFuture = evaluationQueue.poll();
            nextFuture.first().cancel(true);
            pipelines.checkIn(nextFuture.second());
        }
        results.clear();
    }
    
    public void startPipeline() {
        // start up to maxPipeline pipelines
        int maxPipelines = pipelines.maxPipelines;
        boolean isNested = false;
        NestedQuery<Key> nestedQuery = null;
        if (docSource instanceof NestedQueryIterator) {
            if (log.isTraceEnabled()) {
                log.trace("we're in a nested query");
            }
            isNested = true;
            
        }
        
        for (int i = 0; i < maxPipelines && this.docSource.hasNext(); i++) {
            Key keySource = this.docSource.next();
            if (isNested) {
                nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
            }
            if (log.isTraceEnabled()) {
                log.trace("evaluating nested " + nestedQuery);
            }
            evaluate(keySource, this.docSource.document(), nestedQuery, columnFamilies, inclusive);
        }
    }
    
    private void evaluate(Key key, Document document, NestedQuery<Key> nestedQuery, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        if (log.isTraceEnabled()) {
            log.trace("Adding evaluation of " + key + " to pipeline");
        }
        Pipeline pipeline = pipelines.checkOut(key, document, nestedQuery, columnFamilies, inclusive);
        
        evaluationQueue.add(new Tuple2<>(IteratorThreadPoolManager.executeEvaluation(pipeline, pipeline.toString(), env), pipeline));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

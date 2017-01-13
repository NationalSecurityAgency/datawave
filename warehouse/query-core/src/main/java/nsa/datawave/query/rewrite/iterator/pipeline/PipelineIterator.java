package nsa.datawave.query.rewrite.iterator.pipeline;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import nsa.datawave.core.iterators.IteratorThreadPoolManager;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.NestedQuery;
import nsa.datawave.query.rewrite.iterator.NestedQueryIterator;
import nsa.datawave.query.rewrite.iterator.QueryIterator;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpan;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpanCollector;
import nsa.datawave.query.util.Tuple2;

/**
 * This is the iterator that handles the evaluation pipelines. Essentially it will queue up N evaluations. On each hasNext and next call, it will pull the
 * results ready from the top and cache the non-null results in a results queue.
 */
public class PipelineIterator implements Iterator<Entry<Key,Value>> {
    
    private static final Logger log = Logger.getLogger(PipelineIterator.class);
    final protected NestedIterator<Key> docSource;
    final protected PipelinePool pipelines;
    final protected Queue<Tuple2<Future<?>,Pipeline>> evaluationQueue;
    final protected Queue<Entry<Key,Value>> results;
    final protected int maxResults;
    final protected QuerySpanCollector querySpanCollector;
    final protected QuerySpan querySpan;
    protected boolean collectTimingDetails = false;
    protected IteratorEnvironment env;
    
    public PipelineIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env) {
        this.docSource = documents;
        this.pipelines = new PipelinePool(maxPipelines, querySpanCollector, sourceIterator, sourceForDeepCopy, env);
        this.evaluationQueue = new LinkedList<>();
        this.results = new LinkedList<>();
        this.maxResults = maxCachedResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.env = env;
        
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
        Entry<Key,Value> next = getNext(false);
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
    public Entry<Key,Value> next() {
        Entry<Key,Value> next = getNext(true);
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
    private Entry<Key,Value> getNext(boolean remove) {
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
            Entry<Key,Value> next = null;
            if (!results.isEmpty()) {
                if (remove) {
                    next = results.poll();
                } else {
                    next = results.peek();
                }
            }
            return next;
        } catch (InterruptedException | IterationInterruptedException e) {
            // cancel out existing executions, and exit gracefully
            cancel();
            return null;
        } catch (ExecutionException e) {
            // cancel out existing executions and throw the exception up
            // unless interrupted
            cancel();
            // if a interrupted exception, then exit gracefully
            Throwable t = e.getCause();
            if (t instanceof InterruptedException || t instanceof IterationInterruptedException) {
                return null;
            }
            // otherwise pass the exception up
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
        Entry<Key,Value> result = null;
        
        while (!evaluationQueue.isEmpty() && result == null) {
            result = poll();
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
            poll();
        }
    }
    
    /**
     * Poll the next evaluation future, start a new evaluation in its place, queue and return the result. This assumes there is a queued evaluation to get.
     * 
     * @return The next evaluation result
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private Entry<Key,Value> poll() throws InterruptedException, ExecutionException {
        // get the next evaluated result
        Tuple2<Future<?>,Pipeline> nextFuture = evaluationQueue.poll();
        
        // wait for it to complete if not already done
        if (!nextFuture.first().isDone()) {
            long start = System.currentTimeMillis();
            
            nextFuture.first().get();
            
            if (log.isDebugEnabled()) {
                long wait = System.currentTimeMillis() - start;
                log.debug("Waited " + wait + "ms for the top evaluation in a queue of " + evaluationQueue.size() + " pipelines");
            }
        }
        
        // pull the result
        Entry<Key,Value> result = nextFuture.second().getResult();
        
        // return the pipeline for reuse
        pipelines.checkIn(nextFuture.second());
        
        // start a new evaluation if we can
        if (docSource.hasNext()) {
            Key keySource = docSource.next();
            NestedQuery<Key> nestedQuery = null;
            if (docSource instanceof NestedQueryIterator) {
                nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
            }
            
            evaluate(keySource, docSource.document(), nestedQuery);
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
            evaluate(keySource, this.docSource.document(), nestedQuery);
        }
    }
    
    private void evaluate(Key key, Document document, NestedQuery<Key> nestedQuery) {
        Pipeline pipeline = pipelines.checkOut(key, document, nestedQuery);
        
        evaluationQueue.add(new Tuple2<Future<?>,Pipeline>(IteratorThreadPoolManager.executeEvaluation(pipeline, pipeline.toString()), pipeline));
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

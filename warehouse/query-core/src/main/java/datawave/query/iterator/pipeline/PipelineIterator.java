package datawave.query.iterator.pipeline;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;

import datawave.core.iterators.IteratorThreadPoolManager;
import datawave.query.attributes.Document;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.NestedQuery;
import datawave.query.iterator.NestedQueryIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.waitwindow.WaitWindowObserver;
import datawave.query.util.Tuple2;

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
    protected final WaitWindowObserver waitWindowObserver;
    protected Key keyFromWaitWindowOverrun = null;
    protected Entry<Key,Document> result = null;

    public PipelineIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, WaitWindowObserver waitWindowObserver, Collection<ByteSequence> columnFamilies,
                    boolean inclusive) {
        this.docSource = documents;
        this.pipelines = new PipelinePool(maxPipelines, querySpanCollector, sourceIterator, sourceForDeepCopy, env);
        this.evaluationQueue = new LinkedList<>();
        this.results = new LinkedList<>();
        this.maxResults = maxCachedResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.env = env;
        this.yield = yieldCallback;
        this.waitWindowObserver = waitWindowObserver;
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
        // if we have exceeded the wait window, then return true if
        // collectTimingDetails, otherwise yield and return false
        if (this.keyFromWaitWindowOverrun != null) {
            boolean yieldToBeginning = WaitWindowObserver.hasBeginMarker(this.keyFromWaitWindowOverrun);
            result = handleWaitWindowOverrun(this.keyFromWaitWindowOverrun, yieldToBeginning);
        } else {
            result = getNext();
            if (result != null && WaitWindowObserver.hasMarker(result.getKey())) {
                boolean yieldToBeginning = WaitWindowObserver.hasBeginMarker(result.getKey());
                result = handleWaitWindowOverrun(result.getKey(), yieldToBeginning);
            }
        }

        boolean yielded = (this.yield != null) && this.yield.hasYielded();
        if (!yielded && log.isTraceEnabled()) {
            log.trace("PipelineIterator.hasNext() -> " + (result == null ? null : result.getKey()));
        }
        return (!yielded) && (result != null);
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

        if (log.isTraceEnabled()) {
            log.trace("PipelineIterator.next() -> " + (result == null ? null : result.getKey()));
        }
        Entry<Key,Document> returnResult = result;
        result = null;
        return returnResult;
    }

    /**
     * Get the next non-null result from the queue.
     *
     * @return the next non-null entry. null if there are no more entries to get.
     */
    private Entry<Key,Document> getNext() {
        try {
            if (log.isTraceEnabled()) {
                log.trace("getNext start: " + evaluationQueue.size() + " cached: " + results.size());
            }

            // cache the next non-null result if we do not already have one
            if (results.isEmpty()) {
                cacheNextResult();
            }

            if (log.isTraceEnabled()) {
                log.trace("getNext cache: " + evaluationQueue.size() + " cached: " + results.size());
            }

            // flush any completed results to the results queue
            flushCompletedResults();

            if (log.isTraceEnabled()) {
                log.trace("getNext flush: " + evaluationQueue.size() + " cached: " + results.size());
            }

            // ensure that the evaluation queue is filled if there is anything to evaluate
            fillEvaluationQueue();

            if (log.isTraceEnabled()) {
                log.trace("getNext fill: " + evaluationQueue.size() + " cached: " + results.size());
            }

            // get/remove and return the next result, null if we are done
            Entry<Key,Document> next = null;
            if (!results.isEmpty()) {
                next = results.poll();
            }

            return next;
        } catch (WaitWindowOverrunException e) {
            Key yieldKey = e.getYieldKey();
            return handleWaitWindowOverrun(yieldKey, WaitWindowObserver.hasBeginMarker(yieldKey));
        } catch (Exception e) {
            if (yield != null && yield.hasYielded()) {
                // if we yielded, then leave gracefully
                return null;
            }
            // if we have not yielded, then cancel existing evaluations
            cancel();
            log.error("Failed to retrieve evaluation pipeline result", e);
            throw new RuntimeException("Failed to retrieve evaluation pipeline result", e);
        }
    }

    /**
     * poll results from the evaluation queue until we get one that is non-null
     *
     * @throws ExecutionException
     *             for execution exceptions
     * @throws InterruptedException
     *             for interrupted exceptions
     */
    private void cacheNextResult() throws InterruptedException, ExecutionException {
        Entry<Key,Document> result = null;

        while (!evaluationQueue.isEmpty() && result == null) {
            // we must have at least evaluated one thing in order to yield, otherwise we will have not progressed at all
            if (yield != null && lastKeyEvaluated != null) {
                long remainingTimeMs = waitWindowObserver.remainingTimeMs();
                if (remainingTimeMs <= 0) {
                    List<Key> yieldKeys = Collections.singletonList(waitWindowObserver.createYieldKey(lastKeyEvaluated, false));
                    throwExceptionOnWaitWindowOverrun(yieldKeys);
                }
                try {
                    result = poll(remainingTimeMs);
                    // put the result into the queue if non-null
                    if (result != null) {
                        results.add(result);
                    }
                    // ensure that the evaluation queue is filled if there is anything to evaluate
                    fillEvaluationQueue();
                } catch (TimeoutException e) {
                    // lastKeyEvaluated either succeeded and is in results or failed so we can yield past it
                    List<Key> yieldKeys = Collections.singletonList(waitWindowObserver.createYieldKey(lastKeyEvaluated, false));
                    throwExceptionOnWaitWindowOverrun(yieldKeys);
                }
            } else {
                try {
                    result = poll(Long.MAX_VALUE);
                    // put the result into the queue if non-null
                    if (result != null) {
                        results.add(result);
                    }
                    // ensure that the evaluation queue is filled if there is anything to evaluate
                    fillEvaluationQueue();
                } catch (TimeoutException e) {
                    // should be impossible with a Long.MAX_VALUE, but we can wait another 292 million years
                    log.error("We have been waiting for 292 million years, trying again");
                }
            }
        }
    }

    private List<Key> getPossibleYieldKeys(List<Key> additionalYieldKeys) {
        List<Key> possibleYieldKeys = new ArrayList<>();
        // Add additional passed-in yieldKeys
        possibleYieldKeys.addAll(additionalYieldKeys);
        // Create and add a yieldKey for each unevaluated key in the evaluationQueue
        for (Tuple2<Future<?>,Pipeline> t : evaluationQueue) {
            possibleYieldKeys.add(waitWindowObserver.createYieldKey(t.second().getSource().getKey(), true));
        }
        // Create and add a yieldKey for each not-yet returned result in the results list
        for (Entry<Key,Document> r : results) {
            possibleYieldKeys.add(waitWindowObserver.createYieldKey(r.getKey(), true));
        }
        // dedupe and sort possibleYieldKeys
        return possibleYieldKeys.stream().distinct().collect(Collectors.toList());
    }

    private Key findLowestYieldKey(List<Key> possibleYieldKeys) {
        possibleYieldKeys.sort(WaitWindowObserver.keyComparator);
        // if there is more than one key and the lowest is YIELD_TO_END, then we can remove the first
        while (possibleYieldKeys.size() > 1 && WaitWindowObserver.hasEndMarker(possibleYieldKeys.get(0))) {
            possibleYieldKeys.remove(0);
        }
        Key yieldKey;
        if (possibleYieldKeys.size() > 1) {
            yieldKey = waitWindowObserver.lowestYieldKey(possibleYieldKeys);
            if (log.isTraceEnabled()) {
                log.trace("Yielding at:" + yieldKey + " after evaluating keys:" + possibleYieldKeys);
            }
        } else {
            yieldKey = possibleYieldKeys.get(0);
        }
        return yieldKey;
    }

    // If collectTimingDetails == true, then we wil set the (future) yieldKey in waitWindowObserver and return
    // an entry with the yield key and a WAIT_WINDOW_OVERRUN document to which the timing details can be added
    // If collectTimingDetails == false, then we yield and return a null
    private Map.Entry<Key,Document> handleWaitWindowOverrun(Key key, boolean yieldToBeginning) {
        Map.Entry<Key,Document> result = null;
        List<Key> additionalYieldKeys = Collections.singletonList(waitWindowObserver.createYieldKey(key, yieldToBeginning));
        List<Key> possibleYieldKeys = getPossibleYieldKeys(additionalYieldKeys);
        Key yieldKey = findLowestYieldKey(possibleYieldKeys);
        if (collectTimingDetails) {
            if (log.isDebugEnabled()) {
                log.debug("WaitWindowOverrun at " + yieldKey);
            }
            waitWindowObserver.setYieldKey(yieldKey);
            result = new AbstractMap.SimpleEntry<>(yieldKey, WaitWindowObserver.getWaitWindowOverrunDocument());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Yielding at " + yieldKey);
            }
            yield.yield(yieldKey);
        }
        // Either we're yielding or returning a WAIT_WINDOW_OVERRUN document.
        // We're done using the evaluationQueue and results and can cancel and clear
        log.debug("Cancelling remaining evaluations and removing results due to yield");
        cancel();
        return result;
    }

    // This exception should be caught in getNext which will call handleWaitWindowOverrun
    private void throwExceptionOnWaitWindowOverrun(List<Key> yieldKeys) {
        List<Key> possibleYieldKeys = getPossibleYieldKeys(yieldKeys);
        Key yieldKey = findLowestYieldKey(possibleYieldKeys);
        throw new WaitWindowOverrunException(yieldKey);
    }

    /**
     * flush the results from the evaluation queue that are complete up to the max number of cached results
     *
     * @throws ExecutionException
     *             for execution exceptions
     * @throws InterruptedException
     *             for interrupted exceptions
     */
    private void flushCompletedResults() throws InterruptedException, ExecutionException {
        while (!evaluationQueue.isEmpty() && evaluationQueue.peek().first().isDone() && results.size() < this.maxResults) {
            try {
                Entry<Key,Document> result = poll(Long.MAX_VALUE);
                if (result != null) {
                    results.add(result);
                }
            } catch (TimeoutException e) {
                // should be impossible with a Long.MAX_VALUE, but we can wait another 292 million years
                log.error("We have been waiting for 292 million years, trying again");
            }
        }
    }

    /**
     * Poll the next evaluation future, start a new evaluation in its place, queue and return the result. This assumes there is a queued evaluation to get.
     *
     * @param waitMs
     *            time in ms to wait
     * @return The next evaluation result
     * @throws ExecutionException
     *             for execution exceptions
     * @throws InterruptedException
     *             for interrupted exceptions
     * @throws TimeoutException
     *             for timeout exceptions
     */
    private Entry<Key,Document> poll(long waitMs) throws InterruptedException, ExecutionException, TimeoutException {
        // get the next entry on the evaluationQueue but do not remove
        Tuple2<Future<?>,Pipeline> nextFuture = evaluationQueue.peek();

        Entry<Key,Document> nextEntry;
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
            nextEntry = nextFuture.second().getResult();

            if (log.isTraceEnabled()) {
                Key docKey = nextFuture.second().getSource().getKey();
                log.trace("Polling for result from " + docKey + " was " + (nextEntry == null ? "empty" : "successful"));
            }

            // record the last evaluated key
            lastKeyEvaluated = nextFuture.second().getSource().getKey();
            // remove completed Entry<Future<?>,Pipeline> from the evaluation queue
            evaluationQueue.remove();
            // return the pipeline for reuse
            pipelines.checkIn(nextFuture.second());
        } catch (TimeoutException e) {
            // timeout means that we are yielding
            throw e;
        } catch (Exception e) {
            Key docKey = nextFuture.second().getSource().getKey();
            log.error("Failed polling for result from " + docKey, e);
            throw e;
        }
        return nextEntry;
    }

    private void fillEvaluationQueue() {
        // start a new evaluation for any available sources if there is room in the evaluationQueue
        while (docSource.hasNext() && evaluationQueue.size() < pipelines.maxPipelines) {
            Key keySource = docSource.next();
            if (WaitWindowObserver.hasMarker(keySource)) {
                List<Key> yieldKeys = new ArrayList<>();
                yieldKeys.add(waitWindowObserver.createYieldKey(keySource, WaitWindowObserver.hasBeginMarker(keySource)));
                if (lastKeyEvaluated != null) {
                    // lastKeyEvaluated was updated in poll() and may be a valid result
                    yieldKeys.add(lastKeyEvaluated);
                }
                throwExceptionOnWaitWindowOverrun(yieldKeys);
            } else {
                NestedQuery<Key> nestedQuery = null;
                if (docSource instanceof NestedQueryIterator) {
                    nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
                }

                evaluate(keySource, docSource.document(), nestedQuery, columnFamilies, inclusive);
                if (collectTimingDetails) {
                    querySpanCollector.addQuerySpan(querySpan);
                }
            }
        }
    }

    /**
     * Cancel queued evaluations and clear results
     */
    private void cancel() {
        while (!evaluationQueue.isEmpty()) {
            Tuple2<Future<?>,Pipeline> pair = evaluationQueue.poll();
            Future<?> future = pair.first();
            Pipeline pipeline = pair.second();
            future.cancel(false);
            pipeline.waitUntilComplete();
            pipelines.checkIn(pipeline);
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
            if (keySource != null && WaitWindowObserver.hasMarker(keySource)) {
                this.keyFromWaitWindowOverrun = keySource;
                break;
            }
            if (isNested) {
                nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
                if (log.isTraceEnabled()) {
                    log.trace("evaluating nested " + nestedQuery);
                }
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

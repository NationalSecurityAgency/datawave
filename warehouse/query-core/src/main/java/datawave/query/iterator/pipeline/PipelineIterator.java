package datawave.query.iterator.pipeline;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import datawave.core.iterators.IteratorThreadPoolManager;
import datawave.query.attributes.Document;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.function.LogTiming;
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

    private final Logger log = Logger.getLogger(getClass());
    protected final YieldCallback<Key> yieldCallback;
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
    protected WaitWindowOverrunException waitWindowOverrunException = null;
    protected Entry<Key,Document> result = null;
    protected String queryId;

    public PipelineIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, WaitWindowObserver waitWindowObserver, String queryId,
                    Collection<ByteSequence> columnFamilies, boolean inclusive) {
        this.docSource = documents;
        this.pipelines = new PipelinePool(maxPipelines, querySpanCollector, sourceIterator, sourceForDeepCopy, env);
        this.evaluationQueue = new LinkedList<>();
        this.results = new LinkedList<>();
        this.maxResults = maxCachedResults;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.env = env;
        this.yieldCallback = yieldCallback;
        this.waitWindowObserver = waitWindowObserver;
        this.queryId = queryId;
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
        if (this.waitWindowOverrunException != null) {
            result = handleWaitWindowOverrun(this.waitWindowOverrunException);
        } else {
            // getNext() catches and handles WaitWindowOverrunException
            result = getNext();
        }

        boolean yielded = (this.yieldCallback != null) && this.yieldCallback.hasYielded();
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
        if (yieldCallback != null && yieldCallback.hasYielded()) {
            return null;
        }

        if (log.isTraceEnabled()) {
            log.trace("PipelineIterator.next() -> " + (result == null ? null : result.getKey()));
        }
        Entry<Key,Document> returnResult = result;
        result = null;
        if (collectTimingDetails && returnResult != null) {
            // add any collected timing metadata right before returning the result
            addTimingMetadata(returnResult.getValue());
        }
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
            return handleWaitWindowOverrun(e);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (yieldCallback != null && yieldCallback.hasYielded()) {
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
            if (yieldCallback != null && lastKeyEvaluated != null) {
                long remainingTimeMs = waitWindowObserver.remainingTimeMs();
                if (remainingTimeMs <= 0) {
                    throwExceptionOnWaitWindowOverrun(
                                    waitWindowObserver.createYieldKey(lastKeyEvaluated, false, "lastEvaluated in PipelineIterator.cacheNextResult()"));
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
                    throwExceptionOnWaitWindowOverrun(waitWindowObserver.createYieldKey(lastKeyEvaluated, false, "PipelineIterator.cacheNextResult()"));
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

    private List<Pair<Key,String>> getPossibleYieldKeys(List<Pair<Key,String>> additionalYieldKeys) {
        // used to de-dupe the possible keys based on the Key part of the Pair
        Set<Key> keySet = new HashSet<>();
        List<Pair<Key,String>> possibleYieldKeys = new ArrayList<>();
        // Add additional passed-in yieldKeys
        additionalYieldKeys.forEach(pair -> {
            if (!keySet.contains(pair.getLeft())) {
                keySet.add(pair.getLeft());
                possibleYieldKeys.add(pair);
            }
        });
        // Create and add a yieldKey for each unevaluated key in the evaluationQueue
        evaluationQueue.forEach(t -> {
            if (!keySet.contains(t.second().getSource().getKey())) {
                keySet.add(t.second().getSource().getKey());
                possibleYieldKeys.add(waitWindowObserver.createYieldKey(t.second().getSource().getKey(), true,
                                "evaluationQueue in PipelineIterator.getPossibleYieldKeys()"));
            }
        });
        // Create and add a yieldKey for each not-yet returned result in the results list
        results.forEach(r -> {
            if (!keySet.contains(r.getKey())) {
                keySet.add(r.getKey());
                possibleYieldKeys.add(waitWindowObserver.createYieldKey(r.getKey(), true, "results in PipelineIterator.getPossibleYieldKeys()"));
            }
        });
        // dedupe and sort possibleYieldKeys
        return possibleYieldKeys;
    }

    private Pair<Key,String> findLowestYieldKey(List<Pair<Key,String>> possibleYieldKeys) {
        possibleYieldKeys.sort(WaitWindowObserver.keyComparator);
        // if there is more than one key and the lowest is YIELD_TO_END, then we can remove the first
        while (possibleYieldKeys.size() > 1 && WaitWindowObserver.hasEndMarker(possibleYieldKeys.get(0).getLeft())) {
            possibleYieldKeys.remove(0);
        }
        Pair<Key,String> yieldKey;
        if (possibleYieldKeys.size() > 1) {
            yieldKey = waitWindowObserver.lowestYieldKey(possibleYieldKeys);
            if (log.isTraceEnabled()) {
                log.trace("Yielding at:" + yieldKey + " after evaluating keys:" + possibleYieldKeys);
            }
        } else {
            Pair<Key,String> first = possibleYieldKeys.get(0);
            yieldKey = waitWindowObserver.createYieldKey(first.getLeft(), WaitWindowObserver.shouldYieldToBeginning(first.getLeft()), first.getRight());
        }
        return yieldKey;
    }

    /*
     * The original yieldKey will be first and the most recent yieldKey will be last
     */
    public void logYieldKeyHistory(WaitWindowOverrunException e, String prefix) {
        int n = 1;
        for (Pair<Key,String> h : e.getYieldKeyHistory()) {
            log.debug(prefix + " yieldKeyHistory-" + n++ + ": " + h);
        }
    }

    /**
     * handleWaitWindowOverrun for collectTimingDetails = true or false and either yield or return an entry to be used in a WAIT_WINDOW_OVERRUN document
     *
     * If collectTimingDetails == true, then we will set the (future) yieldKey in waitWindowObserver and return an entry with the yield key and a
     * WAIT_WINDOW_OVERRUN document to which the timing details can be added. If collectTimingDetails == false, then we yield and return a null.
     *
     * @param e
     *            - the WaitWindowOverrunException that has been propagated
     *
     * @return either null when yielding or an Entry to be used in a WAIT_WINDOW_OVERRUN document
     */
    private Map.Entry<Key,Document> handleWaitWindowOverrun(WaitWindowOverrunException e) {
        Map.Entry<Key,Document> result = null;
        if (e.getYieldKey().getLeft() == null) {
            // This handles the case where a yield occurs before a valid yieldKey can be determined by creating
            // a Pair<Key, String> yieldKey using the range start key and the description that came with the null Key
            Key rangeStart = waitWindowObserver.getSeekRange().getStartKey();
            String description = e.getYieldKey().getRight();
            Pair<Key,String> yieldKey = waitWindowObserver.createYieldKey(rangeStart, true, description);
            e.setYieldKey(yieldKey);
        }
        List<Pair<Key,String>> additionalYieldKeys = Collections.singletonList(e.getYieldKey());
        List<Pair<Key,String>> possibleYieldKeys = getPossibleYieldKeys(additionalYieldKeys);
        // set the lowestYieldKey into the exception to preserve the yieldKey history
        e.setYieldKey(findLowestYieldKey(possibleYieldKeys));
        if (collectTimingDetails) {
            if (log.isDebugEnabled()) {
                logYieldKeyHistory(e, "WaitWindowOverrun");
                log.debug("WaitWindowOverrun at yieldKey " + e.getYieldKey().getLeft() + " for queryId:" + queryId);
            }
            waitWindowObserver.setYieldKey(e.getYieldKey());
            result = new AbstractMap.SimpleEntry<>(e.getYieldKey().getLeft(), WaitWindowObserver.getWaitWindowOverrunDocument());
        } else {
            if (log.isDebugEnabled()) {
                logYieldKeyHistory(e, "Yielded");
                log.debug("Scan yield at yieldKey " + e.getYieldKey().getLeft() + " for queryId:" + queryId);
            }
            yieldCallback.yield(e.getYieldKey().getLeft());
        }
        // Either we're yielding or returning a WAIT_WINDOW_OVERRUN document.
        // We're done using the evaluationQueue and results and can cancel and clear
        log.debug("Cancelling remaining evaluations and removing results due to yield");
        cancel();
        return result;
    }

    private void throwExceptionOnWaitWindowOverrun(Pair<Key,String> yieldKey) {
        throwExceptionOnWaitWindowOverrun(Collections.singletonList(yieldKey));
    }

    // This exception should be caught in getNext which will call handleWaitWindowOverrun
    private void throwExceptionOnWaitWindowOverrun(List<Pair<Key,String>> yieldKeys) {
        throw new WaitWindowOverrunException(findLowestYieldKey(getPossibleYieldKeys(yieldKeys)));
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
        } catch (TimeoutException | WaitWindowOverrunException e) {
            // either exception means that we are yielding
            // If a WaitWindowOverrunException is thrown, caught, and saved during Pipeline evaluation, then
            // it is re-thrown when getResult() is called and will be caught and handled in getNext()
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
        try {
            while (docSource.hasNext() && evaluationQueue.size() < pipelines.maxPipelines) {
                Key keySource = docSource.next();
                if (waitWindowObserver.waitWindowOverrun()) {
                    List<Pair<Key,String>> yieldKeys = new ArrayList<>();
                    yieldKeys.add(waitWindowObserver.createYieldKey(keySource, WaitWindowObserver.shouldYieldToBeginning(keySource),
                                    "keySource in PipelineIterator.fillEvaluationQueue()"));
                    if (lastKeyEvaluated != null) {
                        // lastKeyEvaluated either succeeded and is in results or failed so we can yield past it
                        yieldKeys.add(waitWindowObserver.createYieldKey(lastKeyEvaluated, false, "lastEvaluated in PipelineIterator.fillEvaluationQueue()"));
                    }
                    throwExceptionOnWaitWindowOverrun(yieldKeys);
                } else {
                    NestedQuery<Key> nestedQuery = null;
                    if (docSource instanceof NestedQueryIterator) {
                        nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
                    }
                    evaluate(keySource, docSource.document(), nestedQuery, columnFamilies, inclusive);
                }
            }
        } finally {
            if (collectTimingDetails) {
                querySpanCollector.addQuerySpan(querySpan);
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

        try {
            for (int i = 0; i < maxPipelines && this.docSource.hasNext(); i++) {
                Key keySource = this.docSource.next();
                if (isNested) {
                    nestedQuery = ((NestedQueryIterator) this.docSource).getNestedQuery();
                    if (log.isTraceEnabled()) {
                        log.trace("evaluating nested " + nestedQuery);
                    }
                }
                evaluate(keySource, this.docSource.document(), nestedQuery, columnFamilies, inclusive);
            }
        } catch (WaitWindowOverrunException e) {
            this.waitWindowOverrunException = e;
        }
    }

    private void evaluate(Key key, Document document, NestedQuery<Key> nestedQuery, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        if (log.isTraceEnabled()) {
            log.trace("Adding evaluation of " + key + " to pipeline");
        }
        Pipeline pipeline = pipelines.checkOut(key, document, nestedQuery, columnFamilies, inclusive);
        String taskName = pipeline.toString() + " for queryId:" + queryId;
        evaluationQueue.add(new Tuple2<>(IteratorThreadPoolManager.executeEvaluation(pipeline, taskName, env), pipeline));
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

    protected void addTimingMetadata(Document document) {
        if (document != null) {
            QuerySpan combinedQuerySpan = querySpanCollector.getCombinedQuerySpan(querySpan, true);
            if (combinedQuerySpan != null) {
                LogTiming.addTimingMetadata(document, combinedQuerySpan);
            }
        }
    }
}

package datawave.query.iterator.pipeline;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

public class SerialIterator extends PipelineIterator {

    private final Logger log = Logger.getLogger(getClass());

    protected Pipeline currentPipeline;

    public SerialIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, WaitWindowObserver waitWindowObserver, String queryId,
                    Collection<ByteSequence> columnFamilies, boolean include) {
        super(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yieldCallback, yieldThresholdMs,
                        waitWindowObserver, queryId, columnFamilies, include);
    }

    @Override
    public boolean hasNext() {
        // if we had already yielded, then leave gracefully
        if (yieldCallback != null && yieldCallback.hasYielded()) {
            return false;
        }

        while (result == null && this.docSource.hasNext() && (yieldCallback == null || !yieldCallback.hasYielded())) {
            try {
                Key docKey = this.docSource.next();
                Document doc = this.docSource.document();
                waitWindowObserver.checkWaitWindow(docKey, true, "docKey in SerialIterator.hasNext()");
                currentPipeline.setSource(Maps.immutableEntry(docKey, doc));
                currentPipeline.run();
                // If an exception is thrown, caught, and saved during Pipeline
                // evaluation, then it is re-thrown when getResult() is called
                result = currentPipeline.getResult();
            } catch (WaitWindowOverrunException e) {
                result = handleWaitWindowOverrun(e);
            }
        }
        return result != null;
    }

    @Override
    public Entry<Key,Document> next() {
        // if we have already yielded, then leave gracefully
        if (yieldCallback != null && yieldCallback.hasYielded()) {
            return null;
        }

        Entry<Key,Document> returnResult = result;
        if (collectTimingDetails && returnResult != null) {
            // add any collected timing metadata right before returning the result
            addTimingMetadata(returnResult.getValue());
        }
        result = null;
        return returnResult;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();

    }

    public void startPipeline() {
        if (this.docSource.hasNext()) {
            try {
                Key docKey = this.docSource.next();
                Document doc = this.docSource.document();
                currentPipeline = pipelines.checkOut(docKey, doc, null, columnFamilies, inclusive);
                currentPipeline.run();
                // If an exception is thrown, caught, and saved during Pipeline
                // evaluation, then it is re-thrown when getResult() is called
                result = currentPipeline.getResult();
                if (null == result) {
                    hasNext();
                }
            } catch (WaitWindowOverrunException e) {
                result = handleWaitWindowOverrun(e);
            }
        } else {
            result = null;
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
        if (collectTimingDetails) {
            waitWindowObserver.setYieldKey(e.getYieldKey());
            result = new AbstractMap.SimpleEntry<>(e.getYieldKey().getLeft(), WaitWindowObserver.getWaitWindowOverrunDocument());
            if (log.isDebugEnabled()) {
                logYieldKeyHistory(e, "WaitWindowOverrun");
                log.debug("WaitWindowOverrun at yieldKey " + e.getYieldKey().getLeft() + " for queryId:" + queryId);
            }
        } else {
            if (log.isDebugEnabled()) {
                logYieldKeyHistory(e, "Yielded");
                log.debug("Scan yield at yieldKey " + e.getYieldKey().getLeft() + " for queryId:" + queryId);
            }
            yieldCallback.yield(e.getYieldKey().getLeft());
        }
        return result;
    }
}

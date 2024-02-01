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
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

public class SerialIterator extends PipelineIterator {

    private static final Logger log = Logger.getLogger(SerialIterator.class);

    protected Pipeline currentPipeline;

    public SerialIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, WaitWindowObserver waitWindowObserver, Collection<ByteSequence> columnFamilies,
                    boolean include) {
        super(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yieldCallback, yieldThresholdMs,
                        waitWindowObserver, columnFamilies, include);
    }

    @Override
    public boolean hasNext() {
        // if we had already yielded, then leave gracefully
        if (yield != null && yield.hasYielded()) {
            return false;
        }

        while (result == null && this.docSource.hasNext()) {
            Key docKey = this.docSource.next();
            Document doc = this.docSource.document();
            if (WaitWindowObserver.hasMarker(docKey) || waitWindowObserver.waitWindowOverrun()) {
                result = handleWaitWindowOverrun(docKey, true);
                break;
            } else {
                currentPipeline.setSource(Maps.immutableEntry(docKey, doc));
                currentPipeline.run();
                result = currentPipeline.getResult();
            }
        }
        return result != null;
    }

    @Override
    public Entry<Key,Document> next() {
        // if we had already yielded, then leave gracefully
        if (yield != null && yield.hasYielded()) {
            return null;
        }

        Entry<Key,Document> returnResult = result;
        result = null;
        return returnResult;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();

    }

    public void startPipeline() {
        if (this.docSource.hasNext()) {
            Key docKey = this.docSource.next();
            Document doc = this.docSource.document();
            if (WaitWindowObserver.hasMarker(docKey) || waitWindowObserver.waitWindowOverrun()) {
                result = handleWaitWindowOverrun(docKey, true);
            } else {
                currentPipeline = pipelines.checkOut(docKey, doc, null, columnFamilies, inclusive);
                currentPipeline.run();
                result = currentPipeline.getResult();
                if (null == result) {
                    hasNext();
                }
            }
        } else {
            result = null;
        }
    }

    // If collectTimingDetails == true, then we wil set the (future) yieldKey in waitWindowObserver and return
    // an entry with the yield key and a WAIT_WINDOW_OVERRUN document to which the timing details can be added
    // If collectTimingDetails == false, then we yield and return a null
    private Map.Entry<Key,Document> handleWaitWindowOverrun(Key docKey, boolean yieldToBeginning) {
        Map.Entry<Key,Document> result = null;
        Key yieldKey = waitWindowObserver.createYieldKey(docKey, yieldToBeginning);
        if (collectTimingDetails) {
            waitWindowObserver.setYieldKey(yieldKey);
            result = new AbstractMap.SimpleEntry<>(yieldKey, WaitWindowObserver.getWaitWindowOverrunDocument());
            if (log.isDebugEnabled()) {
                log.debug("WaitWindowOverrun at " + yieldKey);
            }
        } else {
            yield.yield(yieldKey);
            if (log.isDebugEnabled()) {
                log.debug("Yielding at " + yieldKey);
            }
        }
        return result;
    }
}

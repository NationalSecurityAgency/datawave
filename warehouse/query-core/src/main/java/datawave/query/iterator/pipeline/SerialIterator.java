package datawave.query.iterator.pipeline;

import com.google.common.collect.Maps;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map.Entry;

public class SerialIterator extends PipelineIterator {
    
    private static final Logger log = Logger.getLogger(SerialIterator.class);
    
    protected Pipeline currentPipeline;
    
    protected Entry<Key,Document> result = null;
    
    public SerialIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallback<Key> yieldCallback, long yieldThresholdMs, Collection<ByteSequence> columnFamilies, boolean include) {
        super(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yieldCallback, yieldThresholdMs,
                        columnFamilies, include);
    }
    
    @Override
    public boolean hasNext() {
        // if we had already yielded, then leave gracefully
        if (yield != null && yield.hasYielded()) {
            return false;
        }
        
        if (null == result) {
            long start = System.currentTimeMillis();
            while (this.docSource.hasNext()) {
                Key docKey = this.docSource.next();
                Document doc = this.docSource.document();
                currentPipeline.setSource(Maps.immutableEntry(docKey, doc));
                currentPipeline.run();
                result = currentPipeline.getResult();
                if (null != result)
                    break;
                if (yield != null && ((System.currentTimeMillis() - start) > yieldThresholdMs)) {
                    yield.yield(docKey);
                    if (log.isDebugEnabled()) {
                        log.debug("Yielding at " + docKey);
                    }
                    break;
                }
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
            currentPipeline = pipelines.checkOut(this.docSource.next(), this.docSource.document(), null, columnFamilies, inclusive);
            currentPipeline.run();
            result = currentPipeline.getResult();
            if (null == result) {
                hasNext();
            }
        } else {
            result = null;
        }
    }
}

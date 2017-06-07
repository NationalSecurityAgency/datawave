package nsa.datawave.query.rewrite.iterator.pipeline;

import java.util.Map.Entry;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.iterator.YieldCallbackWrapper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.collect.Maps;

import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.QueryIterator;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpan;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpanCollector;

public class SerialIterator extends PipelineIterator {
    
    protected Pipeline currentPipeline;
    
    protected Entry<Key,Value> result = null;
    
    public SerialIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, QuerySpanCollector querySpanCollector, QuerySpan querySpan,
                    QueryIterator sourceIterator, SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env,
                    YieldCallbackWrapper<Key> yieldCallback, long yieldThresholdMs) {
        super(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yieldCallback, yieldThresholdMs);
    }
    
    @Override
    public boolean hasNext() {
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
                    break;
                }
            }
        }
        return result != null;
    }
    
    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> returnResult = result;
        result = null;
        return returnResult;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
        
    }
    
    public void startPipeline() {
        if (this.docSource.hasNext()) {
            currentPipeline = pipelines.checkOut(this.docSource.next(), this.docSource.document(), null);
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

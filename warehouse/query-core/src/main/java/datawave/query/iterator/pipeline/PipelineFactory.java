package datawave.query.iterator.pipeline;

import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

public class PipelineFactory {

    /**
     * Create a pipeline iterator.
     *
     * @param documents
     *            Document Iterator.
     * @param maxPipelines
     *            maximum number of requested pipelines.
     * @param maxCachedResults
     *            maximum cached results.
     * @param requestSerialPipeline
     *            request for a serial pipeline. In the future this choice may not be honored
     * @param querySpanCollector
     *            query span collector
     * @param querySpan
     *            query span
     * @param sourceIterator
     *            source iterator.
     * @param sourceForDeepCopy
     *            source used for deep copies.
     * @param env
     *            iterator environment
     * @param columnFamilies
     *            column families
     * @param inclusive
     *            inclusive flag
     * @param yield
     *            the yield
     * @param yieldThresholdMs
     *            the yield threshold
     * @return an iterator
     */
    public static PipelineIterator createIterator(NestedIterator<Key> documents, int maxPipelines, int maxCachedResults, boolean requestSerialPipeline,
                    QuerySpanCollector querySpanCollector, QuerySpan querySpan, QueryIterator sourceIterator,
                    SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env, YieldCallback<Key> yield, long yieldThresholdMs,
                    WaitWindowObserver waitWindowObserver, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        if (maxPipelines > 1 && !requestSerialPipeline) {
            return new PipelineIterator(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yield,
                            yieldThresholdMs, waitWindowObserver, columnFamilies, inclusive);
        } else {
            return new SerialIterator(documents, maxPipelines, maxCachedResults, querySpanCollector, querySpan, sourceIterator, sourceForDeepCopy, env, yield,
                            yieldThresholdMs, waitWindowObserver, columnFamilies, inclusive);
        }

    }

}

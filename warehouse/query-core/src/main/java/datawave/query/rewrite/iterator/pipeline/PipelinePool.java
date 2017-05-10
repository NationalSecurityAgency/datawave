package datawave.query.rewrite.iterator.pipeline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.iterator.NestedQuery;
import datawave.query.rewrite.iterator.NestedQueryIterator;
import datawave.query.rewrite.iterator.QueryIterator;
import datawave.query.rewrite.iterator.profile.QuerySpanCollector;

/**
 * This is the pool of pipelines used for evaluation of documents.
 */
public class PipelinePool {
    private static final Logger log = Logger.getLogger(PipelinePool.class);
    final int maxPipelines;
    final Set<Pipeline> checkedOut;
    final List<Pipeline> checkedIn;
    final QuerySpanCollector querySpanCollector;
    protected QueryIterator sourceIterator;
    protected SortedKeyValueIterator<Key,Value> sourceForDeepCopy;
    protected IteratorEnvironment env;
    
    public PipelinePool(int maxPipelines, QuerySpanCollector querySpanCollector, QueryIterator sourceIterator,
                    SortedKeyValueIterator<Key,Value> sourceForDeepCopy, IteratorEnvironment env) {
        this.maxPipelines = maxPipelines;
        this.checkedOut = new HashSet<Pipeline>(maxPipelines);
        this.checkedIn = new ArrayList<Pipeline>(maxPipelines);
        this.querySpanCollector = querySpanCollector;
        this.sourceIterator = sourceIterator;
        this.sourceForDeepCopy = sourceForDeepCopy;
        this.env = env;
    }
    
    /**
     * Checkout a pipeline initialized with the specified document, creating a new pipeline if needed
     * 
     * @param key
     * @param doc
     * @param nestedQuery
     * @return a new pipeline initialized and ready to execute
     */
    public Pipeline checkOut(Key key, Document doc, NestedQuery<Key> nestedQuery) {
        if (log.isTraceEnabled()) {
            log.trace("checkOut(" + key + ") " + nestedQuery);
        }
        Pipeline pipeline = null;
        if (!this.checkedIn.isEmpty()) {
            pipeline = checkedIn.remove(checkedIn.size() - 1);
            if (null != pipeline) {
                NestedQueryIterator<Key> nq = pipeline.getDocumentSpecificSource();
                if (null != nestedQuery) {
                    nq.setCurrentQuery(nestedQuery);
                    pipeline.setSourceIterator(sourceIterator.createDocumentPipeline(sourceForDeepCopy.deepCopy(env), nq, querySpanCollector));
                }
            }
        } else if (checkedIn.size() + checkedOut.size() < maxPipelines) {
            pipeline = new Pipeline(this.querySpanCollector, sourceForDeepCopy.deepCopy(env));
            NestedQueryIterator<Key> nq = pipeline.getDocumentSpecificSource();
            if (null != nestedQuery) {
                nq.setCurrentQuery(nestedQuery);
            }
            pipeline.setSourceIterator(sourceIterator.createDocumentPipeline(sourceForDeepCopy.deepCopy(env), nq, querySpanCollector));
        }
        if (pipeline != null) {
            checkedOut.add(pipeline);
            pipeline.setSource(Maps.immutableEntry(key, doc));
        }
        return pipeline;
    }
    
    /*
     * Checkin a used pipeline.
     */
    public void checkIn(Pipeline pipeline) {
        if (log.isTraceEnabled()) {
            log.trace("checkIn(" + pipeline + ')');
        }
        pipeline.clear();
        checkedOut.remove(pipeline);
        checkedIn.add(pipeline);
    }
}

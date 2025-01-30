package datawave.query.iterator.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.query.iterator.NestedQuery;
import datawave.query.iterator.NestedQueryIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.profile.QuerySpanCollector;

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
        this.checkedOut = new HashSet<>(maxPipelines);
        this.checkedIn = new ArrayList<>(maxPipelines);
        this.querySpanCollector = querySpanCollector;
        this.sourceIterator = sourceIterator;
        this.sourceForDeepCopy = sourceForDeepCopy;
        this.env = env;
    }

    /**
     * Checkout a pipeline initialized with the specified document, creating a new pipeline if needed
     *
     * @param key
     *            a key
     * @param doc
     *            the document
     * @param nestedQuery
     *            the nested query
     * @param inclusive
     *            inclusive boolean flag
     * @param columnFamilies
     *            the column families
     * @return a new pipeline initialized and ready to execute
     */
    public Pipeline checkOut(Key key, Document doc, NestedQuery<Key> nestedQuery, Collection<ByteSequence> columnFamilies, boolean inclusive) {
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
                    pipeline.setSourceIterator(sourceIterator.createDocumentPipeline(getSource(), nq, columnFamilies, inclusive, querySpanCollector));
                }
            }
        } else if (checkedIn.size() + checkedOut.size() < maxPipelines) {
            // this pipeline constructor doesn't actually use any of the objects passed in, so don't even bother
            pipeline = new Pipeline(this.querySpanCollector, null);
            NestedQueryIterator<Key> nq = pipeline.getDocumentSpecificSource();
            if (null != nestedQuery) {
                nq.setCurrentQuery(nestedQuery);
            }
            pipeline.setSourceIterator(sourceIterator.createDocumentPipeline(getSource(), nq, columnFamilies, inclusive, querySpanCollector));
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

    private SortedKeyValueIterator<Key,Value> getSource() {
        if (maxPipelines == 1) {
            // if the pipeline pool services a serial iterator do not deep copy the source
            return sourceForDeepCopy;
        }

        // if more than one pipeline is allowed to run at once, assume a fresh copy of the source is a required
        return sourceForDeepCopy.deepCopy(env);
    }
}

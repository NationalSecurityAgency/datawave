package datawave.query.iterator.profile;

import java.util.Iterator;
import java.util.Map;

import datawave.query.function.LogTiming;
import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

public class PipelineQuerySpanCollectionIterator implements Iterator<Map.Entry<Key,Document>> {

    protected QuerySpanCollector querySpanCollector;
    protected QuerySpan querySpan;
    private Logger log = Logger.getLogger(PipelineQuerySpanCollectionIterator.class);
    private Iterator<Map.Entry<Key,Document>> itr;

    public PipelineQuerySpanCollectionIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Iterator<Map.Entry<Key,Document>> itr) {
        this.itr = itr;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = this.itr.hasNext();
        if (hasNext == false && this.querySpan != null) {
            this.querySpanCollector.addQuerySpan(this.querySpan);
        }
        return hasNext;
    }

    @Override
    public Map.Entry<Key,Document> next() {
        Map.Entry<Key,Document> next = this.itr.next();
        Document document = next.getValue();
        QuerySpan combinedQuerySpan = querySpanCollector.getCombinedQuerySpan(querySpan);
        if (combinedQuerySpan != null) {
            LogTiming.addTimingMetadata(document, combinedQuerySpan);
        }
        return next;
    }

    @Override
    public void remove() {
        this.itr.remove();
    }
}

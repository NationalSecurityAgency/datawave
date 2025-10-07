package datawave.query.iterator.profile;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;

public class PipelineQuerySpanCollectionIterator implements Iterator<Map.Entry<Key,Document>> {

    protected QuerySpanCollector querySpanCollector;
    protected QuerySpan querySpan;
    private Iterator<Map.Entry<Key,Document>> itr;

    public PipelineQuerySpanCollectionIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Iterator<Map.Entry<Key,Document>> itr) {
        this.itr = itr;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
    }

    @Override
    public boolean hasNext() {
        try {
            return this.itr.hasNext();
        } finally {
            if (this.querySpanCollector != null) {
                this.querySpanCollector.addQuerySpan(this.querySpan);
            }
        }
    }

    @Override
    public Map.Entry<Key,Document> next() {
        try {
            return this.itr.next();
        } finally {
            if (this.querySpanCollector != null) {
                this.querySpanCollector.addQuerySpan(this.querySpan);
            }
        }
    }

    @Override
    public void remove() {
        this.itr.remove();
    }
}

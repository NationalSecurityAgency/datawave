package nsa.datawave.query.rewrite.iterator.pipeline;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.iterator.DocumentSpecificNestedIterator;
import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.NestedQueryIterator;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpanCollector;

/**
 * A pipeline that can be executed as a runnable
 */
public class Pipeline implements Runnable {
    
    private static final Logger log = Logger.getLogger(Pipeline.class);
    /**
     * A source list for which the iterator will automatically reset to the beginning upon comodification. This allows us to have an iterator that will always
     * return results as long as elements are added to the list.
     */
    private DocumentSpecificNestedIterator documentSpecificSource = new DocumentSpecificNestedIterator(null);
    
    // the result
    private Entry<Key,Value> result = null;
    // the pipeline
    private Iterator<Entry<Key,Value>> iterator = null;
    
    private QuerySpanCollector querySpanCollector = null;
    
    public Pipeline(QuerySpanCollector querySpanCollector, SortedKeyValueIterator<Key,Value> sourceForDeepCopy) {
        this.querySpanCollector = querySpanCollector;
        this.iterator = null;
    }
    
    public void setSourceIterator(Iterator<Entry<Key,Value>> sourceIter) {
        this.iterator = sourceIter;
    }
    
    public NestedQueryIterator<Key> getDocumentSpecificSource() {
        return documentSpecificSource;
    }
    
    public void setSource(Map.Entry<Key,Document> documentKey) {
        this.documentSpecificSource.setDocumentKey(documentKey);
    }
    
    public void clear() {
        this.result = null;
        this.documentSpecificSource.setDocumentKey(null);
    }
    
    public Entry<Key,Value> getResult() {
        return result;
    }
    
    @Override
    public void run() {
        if (iterator.hasNext()) {
            result = iterator.next();
        } else {
            result = null;
        }
        
        if (log.isTraceEnabled()) {
            log.trace("next() returned " + result);
        }
    }
    
}

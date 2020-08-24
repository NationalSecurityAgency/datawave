package datawave.query.iterator;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.IdentityAggregator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class EventFieldIterator implements NestedIterator<Key> {
    private final Range range;
    private final SortedKeyValueIterator<Key,Value> source;
    private final String field;
    private final AttributeFactory attributeFactory;
    private final IdentityAggregator aggregator;
    private Key key;
    private Document document;
    
    public EventFieldIterator(Range range, SortedKeyValueIterator<Key,Value> source, String field, AttributeFactory attributeFactory,
                    IdentityAggregator aggregator) {
        this.range = range;
        this.source = source;
        this.field = field;
        this.attributeFactory = attributeFactory;
        this.aggregator = aggregator;
    }
    
    @Override
    public void initialize() {
        try {
            source.seek(range, Collections.emptyList(), false);
            build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Key move(Key minimum) {
        return null;
    }
    
    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.emptySet();
    }
    
    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.emptySet();
    }
    
    @Override
    public Document document() {
        return document;
    }
    
    @Override
    public boolean isContextRequired() {
        return false;
    }
    
    @Override
    public void setContext(Key context) {
        // no-op
    }
    
    @Override
    public boolean hasNext() {
        return key != null;
    }
    
    @Override
    public Key next() {
        Key toReturn = key;
        key = null;
        
        return toReturn;
    }
    
    private void build() {
        // loop over all key/values collecting the ones that need to be collected from the event
        while (source.hasTop()) {
            DatawaveKey datawaveKey = new DatawaveKey(source.getTopKey());
            try {
                if (JexlASTHelper.removeGroupingContext(datawaveKey.getFieldName()).equals(field)) {
                    if (key == null) {
                        document = new Document();
                        key = aggregator.apply(source, document, attributeFactory);
                        
                        // only return a key if something was added to the document, documents that only contain Document.DOCKEY_FIELD_NAME
                        // should not be returned
                        if (document.size() == 1 && document.get(Document.DOCKEY_FIELD_NAME) != null) {
                            key = null;
                            
                            // empty the document
                            document.remove(Document.DOCKEY_FIELD_NAME);
                        }
                    }
                }
                
                source.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

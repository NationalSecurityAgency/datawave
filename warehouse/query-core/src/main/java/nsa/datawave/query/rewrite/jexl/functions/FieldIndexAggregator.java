package nsa.datawave.query.rewrite.jexl.functions;

import java.io.IOException;

import nsa.datawave.query.rewrite.attributes.AttributeFactory;
import nsa.datawave.query.rewrite.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public interface FieldIndexAggregator {
    /**
     * Applies the aggregation function to the keys returned by the iterator.
     */
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException;
    
    /**
     * Applies the aggregation function to the keys returned by the iterator. This also has the side effect of populating the document with the field and value
     * at each key's visibility level. This is intended to be used in the case of having index only fields.
     * 
     * @param itr
     * @param doc
     * @param attrs
     * @return
     */
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException;
}

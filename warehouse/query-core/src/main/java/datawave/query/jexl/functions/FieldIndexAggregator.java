package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.Collection;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public interface FieldIndexAggregator {
    /**
     * Applies the aggregation function to the keys returned by the iterator.
     */
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException;
    
    /**
     * Applies the aggregation function to the keys returned by the iterator, may seek the underlying source
     * 
     * @param itr
     * @param current
     * @param columnFamilies
     * @param includeColumnFamilies
     * @return
     * @throws IOException
     */
    Key apply(SortedKeyValueIterator<Key,Value> itr, Range current, Collection<ByteSequence> columnFamilies, boolean includeColumnFamilies) throws IOException;
    
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

package datawave.query.iterator;

import java.io.IOException;

import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator that can return documents
 */
public interface DocumentIterator extends SortedKeyValueIterator<Key,Value> {
    // get the document
    Document document();
    
    /**
     * Move the iterator forward to the first Key great than or equal to pointer
     *
     * @param pointer
     * @throws IOException
     * @throws IllegalStateException
     *             if getTopKey() is greater than or equal to pointer
     */
    void move(Key pointer) throws IOException;
}

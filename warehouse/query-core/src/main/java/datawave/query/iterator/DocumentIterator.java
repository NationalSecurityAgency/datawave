package datawave.query.iterator;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.query.attributes.Document;

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
     *            a pointer
     * @throws IOException
     *             * @return an iterator visitor
     */
    void move(Key pointer) throws IOException;
}

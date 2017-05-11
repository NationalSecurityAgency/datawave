package datawave.query.rewrite.iterator;

import java.io.IOException;

import datawave.query.rewrite.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator that can return documents
 */
public interface DocumentIterator extends SortedKeyValueIterator<Key,Value> {
    // get the document
    public Document document();
    
    // move forward
    public void move(Key pointer) throws IOException;
}

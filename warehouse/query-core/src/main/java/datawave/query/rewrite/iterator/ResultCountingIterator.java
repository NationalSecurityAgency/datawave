package datawave.query.rewrite.iterator;

import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map.Entry;
import datawave.data.type.util.NumericalEncoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Created on 9/6/16.
 */
public class ResultCountingIterator implements Iterator<Entry<Key,Value>> {
    volatile private long resultCount = 0;
    private Iterator<Entry<Key,Value>> serializedDocuments = null;
    
    public ResultCountingIterator(Iterator<Entry<Key,Value>> serializedDocuments, long resultCount) {
        this.serializedDocuments = serializedDocuments;
        this.resultCount = resultCount;
    }
    
    @Override
    public boolean hasNext() {
        return serializedDocuments.hasNext();
    }
    
    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> next = serializedDocuments.next();
        if (next != null) {
            Key key = next.getKey();
            resultCount++;
            Key resultKey = new Key(key.getRow(), new Text(NumericalEncoder.encode(Long.toString(resultCount)) + '\0' + key.getColumnFamily().toString()),
                            key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
            next = Maps.immutableEntry(resultKey, next.getValue());
        }
        return next;
    }
    
    @Override
    public void remove() {
        serializedDocuments.remove();
    }
}

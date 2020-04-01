package datawave.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class FrequencyColumnIterator extends TransformingIterator {
    
    public FrequencyColumnIterator() {};
    
    public FrequencyColumnIterator(FrequencyColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    protected PartialKey getKeyPrefix() {
        return null;
    }
    
    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator, KVBuffer kvBuffer) throws IOException {
        Key topKey = null;
        
        while (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            if (topKey != null) {
                Value value = sortedKeyValueIterator.getTopValue();
                Text columnQualifier = topKey.getColumnQualifier();
                this.replaceColumnQualifier(topKey, new Text("csv"));
                // byte[] buffer = new byte[columnQualifier.getBytes().length + value.get().length];
                value.set(columnQualifier.getBytes());
            }
            sortedKeyValueIterator.next();
        }
    }
    
}

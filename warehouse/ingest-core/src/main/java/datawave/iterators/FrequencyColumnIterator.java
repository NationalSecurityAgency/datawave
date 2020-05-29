package datawave.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;
import scala.util.control.Exception;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class FrequencyColumnIterator extends TransformingIterator {
    
    public FrequencyColumnIterator() {};
    
    public FrequencyColumnIterator(FrequencyColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    protected PartialKey getKeyPrefix() {
        return PartialKey.ROW_COLFAM;
    }
    
    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator, KVBuffer kvBuffer) throws IOException {
        Key newKey = null;
        StringBuilder newValueSb = new StringBuilder();
        
        while (sortedKeyValueIterator.hasTop()) {
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            Key oldKey = sortedKeyValueIterator.getTopKey();
            if (newKey == null)
                newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text("csv"));
            Value oldValue = sortedKeyValueIterator.getTopValue();
            newValueSb = newValueSb.append(Arrays.toString(cq.getBytes()));
            newValueSb.append(Arrays.toString(oldValue.get()));
            sortedKeyValueIterator.next();
        }
        
        kvBuffer.append(newKey, new Value(newValueSb.toString().getBytes()));
        
    }
    
}

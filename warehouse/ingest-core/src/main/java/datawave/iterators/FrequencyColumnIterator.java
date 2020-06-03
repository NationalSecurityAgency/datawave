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
import java.util.Arrays;
import java.util.Map;

public class FrequencyColumnIterator extends TransformingIterator {
    public static String COL_QUAL_PREFIX = "compressed-";
    
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
        Long numRecords = 0L;
        Key topKey = null;
        Value topValue = null;
        
        if (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
        }
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            Key oldKey = sortedKeyValueIterator.getTopKey();
            Value oldValue = sortedKeyValueIterator.getTopValue();
            
            if (!cq.toString().startsWith(COL_QUAL_PREFIX + cq.toString().substring(0, 3))) {
                newValueSb.append(cq);
                newValueSb.append(oldValue);
                if (newKey == null)
                    newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text(COL_QUAL_PREFIX + cq.toString().substring(0, 3)));
                
            } else {
                newValueSb.append(oldValue);
                newKey = oldKey;
            }
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1)
            kvBuffer.append(newKey, new Value(new Text(String.valueOf(newValueSb))));
        else if (numRecords == 1) {
            kvBuffer.append(topKey, topValue);
            log.info("Range did not need to be transformed  (ran identity transform");
        }
        
    }
}

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
        Long numRecords = 0L;
        Key topKey = null;
        Value topValue = null;
        boolean compressedRecord = false;
        if (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
            if (String.valueOf(topValue.get()).startsWith("compressed")) {
                compressedRecord = true;
            }
            
        }
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            Key oldKey = sortedKeyValueIterator.getTopKey();
            Value oldValue = sortedKeyValueIterator.getTopValue();
            if (newKey == null)
                newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text("compressed"));
            newValueSb = newValueSb.append(Arrays.toString(cq.getBytes()));
            newValueSb.append(Arrays.toString(oldValue.get()));
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1)
            kvBuffer.append(newKey, new Value(new Text(String.valueOf(newValueSb))));
        else if (numRecords == 1) {
            if (!compressedRecord)
                kvBuffer.append(topKey, topValue);
            log.info("Range did not need to be transformed  (ran identity transform");
        }
        
    }
}

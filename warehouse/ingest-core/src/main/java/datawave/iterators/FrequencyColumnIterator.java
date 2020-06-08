package datawave.iterators;

import datawave.query.util.FrequencyFamilyCounter;
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
import java.util.HashMap;
import java.util.Map;

public class FrequencyColumnIterator extends TransformingIterator {
    public static String COL_QUAL_PREFIX = "compressed-";
    public static String COL_QUAL_TOTAL = "total";
    private HashMap<String,Long> qualifierToFrequencyValueMap = new HashMap<>();
    private FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter();
    
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
        Long numRecords = 0L;
        Key topKey = null;
        Value topValue = null;
        frequencyFamilyCounter.clear();
        qualifierToFrequencyValueMap.clear();
        
        if (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
        }
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            Key oldKey = sortedKeyValueIterator.getTopKey();
            Value oldValue = sortedKeyValueIterator.getTopValue();
            
            if (cq.toString().startsWith(COL_QUAL_PREFIX)) {
                log.info("Aggregate key is " + oldKey);
                newKey = oldKey;
                frequencyFamilyCounter.deserializeCompressedValue(oldValue);
            } else {
                
                if (!cq.toString().startsWith(COL_QUAL_TOTAL))
                    frequencyFamilyCounter.insertIntoMap(cq.toString(), oldValue.toString());
                
                if (newKey == null)
                    newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text(COL_QUAL_PREFIX + cq.toString().substring(0, 3)));
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
            if (newKey.getColumnQualifier().toString().startsWith(COL_QUAL_PREFIX))
                kvBuffer.append(new Key(newKey.getRow(), newKey.getColumnFamily(), new Text(COL_QUAL_TOTAL)),
                                new Value(Long.toString(frequencyFamilyCounter.getTotal())));
        } else if (numRecords == 1) {
            kvBuffer.append(topKey, topValue);
            if (topKey.getColumnQualifier().toString().startsWith(COL_QUAL_PREFIX))
                kvBuffer.append(new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(COL_QUAL_TOTAL)),
                                new Value(Long.toString(frequencyFamilyCounter.getTotal())));
            log.info("Range did not need to be transformed  (ran identity transform");
        }
        
        log.info(" Number of key values iterated is " + numRecords);
        
    }
    
}

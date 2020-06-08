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
import java.util.HashMap;
import java.util.Map;

public class FrequencyColumnIterator extends TransformingIterator {
    public static String COL_QUAL_PREFIX = "compressed-";
    public static String COL_QUAL_TOTAL = "total";
    private long total = 0L;
    private HashMap<String,Long> qualifierToFrequencyValueMap = new HashMap<>();
    
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
        // StringBuilder newValueSb = new StringBuilder();
        Long numRecords = 0L;
        total = 0L;
        Key topKey = null;
        Value topValue = null;
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
            
            log.info("Top key: " + oldKey + " Top value: " + oldValue);
            
            if (cq.toString().startsWith(COL_QUAL_PREFIX)) {
                log.info("Aggregate key is " + oldKey);
                newKey = oldKey;
                log.info("deserialize old value " + oldValue);
                
                deserializeCompressedValue(oldValue);
            } else {
                /*
                 * newValueSb.append(cq); newValueSb.append("^"); newValueSb.append(oldValue); newValueSb.append("|");
                 */
                if (!cq.toString().startsWith(COL_QUAL_TOTAL))
                    insertIntoMap(cq.toString(), oldValue.toString());
                
                if (newKey == null)
                    newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text(COL_QUAL_PREFIX + cq.toString().substring(0, 3)));
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            kvBuffer.append(newKey, serialize());
            if (newKey.getColumnQualifier().toString().startsWith(COL_QUAL_PREFIX))
                kvBuffer.append(new Key(newKey.getRow(), newKey.getColumnFamily(), new Text(COL_QUAL_TOTAL)), new Value(Long.toString(total)));
        } else if (numRecords == 1) {
            kvBuffer.append(topKey, topValue);
            if (topKey.getColumnQualifier().toString().startsWith(COL_QUAL_PREFIX))
                kvBuffer.append(new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(COL_QUAL_TOTAL)), new Value(Long.toString(total)));
            log.info("Range did not need to be transformed  (ran identity transform");
        }
        
        log.info(" Number of key values iterated is " + numRecords);
        
    }
    
    private void deserializeCompressedValue(Value oldValue) {
        String[] kvps = oldValue.toString().split("|");
        log.info("deserializeCompressedValue: there are " + kvps.length + " key value pairs.");
        for (String kvp : kvps) {
            String[] pair = kvp.split("^");
            if (pair.length == 2) {
                log.info("deserializeCompressedValue -- cq: " + pair[0] + " value: " + pair[1]);
                String key = pair[0];
                String value = pair[1];
                log.info("deserializeCompressedValue key: " + pair[0] + " value: " + pair[2]);
                insertIntoMap(key, value);
                
            }
        }
        log.info("The contents of the frequency map are " + qualifierToFrequencyValueMap.toString());
    }
    
    private void insertIntoMap(String key, String value) {
        long parsedLong;
        
        log.info("inserting key: " + key + " value: " + value);
        if (value.isEmpty())
            return;
        
        try {
            parsedLong = Long.parseLong(value);
            total += parsedLong;
        } catch (Exception e) {
            log.error("Could not parse " + value + " to long for this key " + key, e);
            return;
        }
        
        try {
            
            if (!qualifierToFrequencyValueMap.containsKey(key))
                qualifierToFrequencyValueMap.put(key, parsedLong);
            else {
                
                long lastValue = qualifierToFrequencyValueMap.get(key);
                qualifierToFrequencyValueMap.put(key, lastValue + parsedLong);
            }
        } catch (Exception e) {
            log.error("Error inserting into map", e);
        }
    }
    
    private Value serialize() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,Long> entry : qualifierToFrequencyValueMap.entrySet()) {
            sb.append(entry.getKey()).append("^").append(entry.getValue()).append("|");
        }
        
        return new Value(sb.toString());
    }
    
}

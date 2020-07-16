package datawave.iterators;

import datawave.data.ColumnFamilyConstants;
import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class FrequencyColumnIterator extends TransformingIterator {
    
    private FrequencyFamilyCounter frequencyFamilyCounter;
    
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
        frequencyFamilyCounter = new FrequencyFamilyCounter();
        
        log.trace("Transforming range for key " + sortedKeyValueIterator.getTopKey().getRow().toString(), new Exception());
        
        Key lastKey = null, aggregatedKey = null;
        Value lastValue = null;
        Value aggregatedValue = null;
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            lastKey = sortedKeyValueIterator.getTopKey();
            lastValue = sortedKeyValueIterator.getTopValue();
            
            if (cq.toString().startsWith(MetadataHelper.COL_QUAL_PREFIX)) {
                newKey = lastKey;
                frequencyFamilyCounter.deserializeCompressedValue(lastValue);
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() == 0)
                    log.error("Compressed value was not deserialized properly");
                aggregatedValue = lastValue;
                aggregatedKey = lastKey;
                log.trace("Aggregate Key: " + aggregatedKey.toStringNoTime());
                
            } else {
                
                frequencyFamilyCounter.aggregateRecord(cq.toString(), lastValue.toString());
                
                String newColumnQualifier = MetadataHelper.COL_QUAL_PREFIX + cq.toString().substring(0, 3);
                
                if (newKey == null) {
                    newKey = new Key(lastKey.getRow(), lastKey.getColumnFamily(), new Text(newColumnQualifier));
                    log.trace("Creating new key for aggregated frequency records " + newKey.toStringNoTime());
                }
                
                log.trace("Non-compressed Key: " + lastKey.toStringNoTime());
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            try {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() > 0) {
                    kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
                    log.trace(numRecords + " frequency records for " + newKey.toStringNoTime().replaceAll("false", "") + " were serialized");
                }
                
            } catch (Exception e) {
                log.error("Could not serialize frequency range properly for key " + newKey.toString(), e);
                if (aggregatedValue != null && aggregatedKey != null) {
                    kvBuffer.append(aggregatedKey, aggregatedValue);
                    log.error("Number of records " + numRecords + " " + "Should not have insert aggregate record like this");
                }
            }
        } else if (numRecords == 1) {
            if (aggregatedValue != null && aggregatedKey != null) {
                kvBuffer.append(aggregatedKey, frequencyFamilyCounter.serialize());
                log.trace("Number of records is 1 Key is: " + aggregatedKey.toStringNoTime() + " - Range tranformed a single aggregated range.");
            } else {
                kvBuffer.append(lastKey, lastValue);
                log.trace("Number of records is 1 Key is: " + lastKey.toStringNoTime() + " - Range did not need to be transformed  (ran identity transform)",
                                new Exception());
            }
        }
        
        log.trace(" Number of key values iterated is " + numRecords);
        
    }
    
}

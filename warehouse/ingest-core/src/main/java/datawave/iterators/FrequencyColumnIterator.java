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
        
        Key topKey = null, aggregatedKey = null;
        Value topValue = null;
        Value aggregatedValue = null;
        boolean isFrequencyRange = false;
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
            
            // Just do an identity transform if there it is not a Frequency Column
            if (!topKey.getColumnFamily().toString().equals(ColumnFamilyConstants.COLF_F.toString())) {
                kvBuffer.append(sortedKeyValueIterator.getTopKey(), sortedKeyValueIterator.getTopValue());
                sortedKeyValueIterator.next();
                isFrequencyRange = false;
                continue;
            } else {
                isFrequencyRange = true;
            }
            
            if (cq.toString().startsWith(MetadataHelper.COL_QUAL_PREFIX)) {
                newKey = topKey;
                frequencyFamilyCounter.deserializeCompressedValue(topValue);
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() == 0)
                    log.error("Compressed value was not deserialized properly");
                aggregatedValue = topValue;
                aggregatedKey = topKey;
                log.trace("Aggregate Key: " + aggregatedKey.toStringNoTime());
                
            } else {
                
                frequencyFamilyCounter.aggregateRecord(cq.toString(), topValue.toString());
                
                String newColumnQualifier = MetadataHelper.COL_QUAL_PREFIX + cq.toString().substring(0, 3);
                
                if (newKey == null) {
                    newKey = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(newColumnQualifier));
                    log.trace("Creating new key for aggregated frequency records " + newKey.toStringNoTime());
                }
                
                log.trace("Non-compressed Key: " + topKey.toStringNoTime());
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (!isFrequencyRange)
            return;
        
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
                kvBuffer.append(topKey, topValue);
                log.trace("Number of records is 1 Key is: " + topKey.toStringNoTime() + " - Range did not need to be transformed  (ran identity transform)",
                                new Exception());
            }
        }
        
        log.trace(" Number of key values iterated is " + numRecords);
        
    }
    
}

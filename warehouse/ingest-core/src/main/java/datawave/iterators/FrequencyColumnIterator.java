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
        Key topKey = null;
        Value topValue = null;
        frequencyFamilyCounter = new FrequencyFamilyCounter();
        
        log.info("Transforming range for key " + sortedKeyValueIterator.getTopKey().getRow().toString(), new Exception());
        
        if (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
        }
        
        Key lastKey, aggregatedKey = null;
        Value lastValue;
        Value aggregatedValue = null;
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            lastKey = sortedKeyValueIterator.getTopKey();
            lastValue = sortedKeyValueIterator.getTopValue();
            
            if (cq.toString().startsWith(MetadataHelper.COL_QUAL_PREFIX)) {
                log.info("Aggregate key is " + lastKey);
                newKey = lastKey;
                frequencyFamilyCounter.deserializeCompressedValue(lastValue);
                aggregatedValue = lastValue;
                aggregatedKey = lastKey;
            } else {
                frequencyFamilyCounter.aggregateRecord(cq.toString(), lastValue.toString());
                
                String newColumnQualifier = MetadataHelper.COL_QUAL_PREFIX + cq.toString().substring(0, 3);
                
                if (newKey == null) {
                    newKey = new Key(lastKey.getRow(), lastKey.getColumnFamily(), new Text(newColumnQualifier));
                    log.info("Creating new key for aggregated frequency records " + newKey.toStringNoTime());
                }
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            try {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() > 0) {
                    kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
                    log.info(numRecords + " for " + newKey.toString() + " were serialized");
                }
                
            } catch (Exception e) {
                log.error("Could not serialize frequency range properly for key " + newKey.toString(), e);
                if (aggregatedValue != null && aggregatedKey != null) {
                    kvBuffer.append(aggregatedKey, aggregatedValue);
                    log.info("Number of records " + numRecords + " " + "Should not have insert aggregate record like this");
                }
            }
        } else if (numRecords == 1) {
            if (aggregatedValue != null && aggregatedKey != null) {
                kvBuffer.append(aggregatedKey, aggregatedValue);
                log.info("Number of records " + numRecords + " " + aggregatedKey.toStringNoTime() + "Range tranformed a single aggregated range.",
                                new Exception());
            } else {
                kvBuffer.append(topKey, topValue);
                log.info("Number of records " + numRecords + " " + topKey.toStringNoTime() + "Range did not need to be transformed  (ran identity transform)",
                                new Exception());
            }
        } else {
            if (topKey != null && topValue != null) {
                kvBuffer.append(topKey, topValue);
                log.info("Number of records " + numRecords + " " + topKey.toStringNoTime()
                                + " - Range did not need to be transformed  (ran identity transform)");
            } else {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() > 0) {
                    kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
                    log.info(numRecords + " for " + newKey.toString() + " were serialized");
                }
            }
            
        }
        
        log.info(" Number of key values iterated is " + numRecords);
        
    }
    
}

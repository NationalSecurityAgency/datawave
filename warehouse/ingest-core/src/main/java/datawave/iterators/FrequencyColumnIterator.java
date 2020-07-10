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
        String aggregatedColumnQualifier = null;
        frequencyFamilyCounter = new FrequencyFamilyCounter();
        
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
                aggregatedColumnQualifier = cq.toString();
                log.info("Aggregate key is " + lastKey, new Exception());
                // TODO - Need to check if newKey is not null and another aggregate record for another datatype needs to be generated.
                newKey = lastKey;
                frequencyFamilyCounter.deserializeCompressedValue(lastValue);
                aggregatedValue = lastValue;
                aggregatedKey = lastKey;
            } else {
                frequencyFamilyCounter.aggregateRecord(cq.toString(), lastValue.toString());
                
                String newColumnQualifier = MetadataHelper.COL_QUAL_PREFIX + cq.toString().substring(0, 3);
                
                if (aggregatedColumnQualifier != null && !aggregatedColumnQualifier.equals(newColumnQualifier))
                    log.error("There are multiple aggregated datatypes for this row and this needs be handled");
                
                if (newKey == null)
                    newKey = new Key(lastKey.getRow(), lastKey.getColumnFamily(), new Text(newColumnQualifier));
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            try {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() > 0)
                    kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
            } catch (Exception e) {
                log.error("Could not serialize frequency range properly for key " + newKey.toString(), e);
                if (aggregatedValue != null && aggregatedKey != null) {
                    kvBuffer.append(aggregatedKey, aggregatedValue);
                    log.info("Should not have insert aggregate record like this", new Exception());
                }
            }
        } else if (numRecords == 1) {
            kvBuffer.append(topKey, topValue);
            log.info("Range did not need to be transformed  (ran identity transform)", new Exception());
        }
        
        log.info(" Number of key values iterated is " + numRecords);
        
    }
    
}

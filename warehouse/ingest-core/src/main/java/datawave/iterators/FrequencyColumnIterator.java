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
    public static final String COL_QUAL_TOTAL = "total";
    private FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter(false);
    
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
        frequencyFamilyCounter.clear();
        
        if (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
        }
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            Key oldKey = sortedKeyValueIterator.getTopKey();
            Value oldValue = sortedKeyValueIterator.getTopValue();
            
            if (cq.toString().startsWith(MetadataHelper.COL_QUAL_PREFIX)) {
                aggregatedColumnQualifier = cq.toString();
                log.info("Aggregate key is " + oldKey);
                // TODO - Need to check if newKey is not null and another aggregate record for another datatype needs to be generated.
                newKey = oldKey;
                frequencyFamilyCounter.deserializeCompressedValue(oldValue);
            } else {
                
                if (!cq.toString().startsWith(COL_QUAL_TOTAL))
                    frequencyFamilyCounter.aggregateRecord(cq.toString(), oldValue.toString());
                
                String newColumnQualifier = MetadataHelper.COL_QUAL_PREFIX + cq.toString().substring(0, 3);
                
                if (aggregatedColumnQualifier != null && !aggregatedColumnQualifier.equals(newColumnQualifier))
                    log.error("There is multiple aggregated datatypes for this row and this needs be handled");
                
                if (newKey == null)
                    newKey = new Key(oldKey.getRow(), oldKey.getColumnFamily(), new Text(newColumnQualifier));
                
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (numRecords > 1) {
            kvBuffer.append(newKey, frequencyFamilyCounter.serialize());
        } else if (numRecords == 1) {
            kvBuffer.append(topKey, topValue);
            log.info("Range did not need to be transformed  (ran identity transform");
        }
        
        log.info(" Number of key values iterated is " + numRecords);
        
    }
    
}

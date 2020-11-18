package datawave.iterators;

import datawave.data.ColumnFamilyConstants;
import datawave.query.util.Frequency;
import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import datawave.query.util.YearMonthDay;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class IndexColumnIterator extends TransformingIterator {
    
    private FrequencyFamilyCounter frequencyFamilyCounter;
    // TODO Figure out how to keep the rowIdToCompressedFreqCQMap from getting too large
    // TODO maybe we do not have to worry about that from happening.
    private HashMap<String,FrequencyFamilyCounter> rowIdToCompressedFreqCQMap = new HashMap<>();
    private String ageOffDate = "19000101";
    
    public IndexColumnIterator() {}
    
    public IndexColumnIterator(IndexColumnIterator aThis, IteratorEnvironment environment) {
        super();
        setSource(aThis.getSource().deepCopy(environment));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.get("ageOffDate") != null)
            ageOffDate = options.get("ageOffDate");
    }
    
    @Override
    protected PartialKey getKeyPrefix() {
        return PartialKey.ROW_COLFAM_COLQUAL_COLVIS;
    }
    
    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator, KVBuffer kvBuffer) throws IOException {
        Long numRecords = 0L;
        frequencyFamilyCounter = new FrequencyFamilyCounter();
        FrequencyFamilyCounter tempFrequencyFamilyCounter = new FrequencyFamilyCounter();
        
        if (log.isTraceEnabled())
            log.trace("Transforming range for key " + sortedKeyValueIterator.getTopKey().getRow().toString(), new Exception());
        
        Key topKey = null, aggregatedKey = null;
        Value topValue = null;
        Value aggregatedValue = null;
        boolean isIndexRange = false;
        boolean startingNewRange = true;
        
        while (sortedKeyValueIterator.hasTop()) {
            numRecords++;
            Text cq = sortedKeyValueIterator.getTopKey().getColumnQualifier();
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
            
            // Just do an identity transform if there it is not a Index Column
            if (!topKey.getColumnFamily().toString().equals(ColumnFamilyConstants.COLF_I.toString())) {
                kvBuffer.append(sortedKeyValueIterator.getTopKey(), sortedKeyValueIterator.getTopValue());
                sortedKeyValueIterator.next();
                isIndexRange = false;
                continue;
            } else {
                isIndexRange = true;
            }
            
            if (startingNewRange) {
                startingNewRange = false;
                frequencyFamilyCounter.deserializeCompressedValue(topValue);
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() == 0)
                    log.error("Compressed value was not deserialized properly");
                aggregatedValue = topValue;
                aggregatedKey = topKey;
                
                if (log.isTraceEnabled())
                    log.trace("Aggregate Key: " + aggregatedKey.toStringNoTime());
                
                if (!rowIdToCompressedFreqCQMap.containsKey(aggregatedKey.getRow() + cq.toString()))
                    rowIdToCompressedFreqCQMap.put(aggregatedKey.getRow().toString() + cq.toString(), frequencyFamilyCounter);
                else {
                    FrequencyFamilyCounter previousCounter = rowIdToCompressedFreqCQMap.get(aggregatedKey.getRow() + cq.toString());
                    for (Map.Entry<YearMonthDay,Frequency> entry : previousCounter.getDateToFrequencyValueMap().entrySet()) {
                        if (ageOffDate.compareTo(entry.getKey().getYyyymmdd()) < 0)
                            frequencyFamilyCounter.aggregateRecord(entry.getKey().getYyyymmdd(), entry.getValue().getValue());
                    }
                    rowIdToCompressedFreqCQMap.remove(aggregatedKey.getRow() + cq.toString());
                    rowIdToCompressedFreqCQMap.put(aggregatedKey.getRow() + cq.toString(), frequencyFamilyCounter);
                    
                }
                
            } else {
                tempFrequencyFamilyCounter.clear();
                tempFrequencyFamilyCounter.deserializeCompressedValue(topValue);
                
                for (Map.Entry<YearMonthDay,Frequency> entry : tempFrequencyFamilyCounter.getDateToFrequencyValueMap().entrySet()) {
                    if (ageOffDate.compareTo(entry.getKey().getYyyymmdd()) < 0)
                        frequencyFamilyCounter.aggregateRecord(entry.getKey().getYyyymmdd(), entry.getValue().getValue());
                    // SummingCombiner.VAR_LEN_ENCODER.decode(topValue.get()).intValue()
                }
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (!isIndexRange)
            return;
        
        if (numRecords > 1) {
            try {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() > 0) {
                    kvBuffer.append(topKey, frequencyFamilyCounter.serialize());
                    log.trace(numRecords + " frequency records for " + topKey.toStringNoTime().replaceAll("false", "") + " were serialized");
                }
                
            } catch (Exception e) {
                log.error("Could not serialize frequency range properly for key " + topKey.toString(), e);
                if (aggregatedValue != null && aggregatedKey != null) {
                    kvBuffer.append(aggregatedKey, aggregatedValue);
                    log.error("Number of records " + numRecords + " " + "Should not have insert aggregate record like this");
                }
            }
        } else if (numRecords == 1) {
            if (aggregatedValue != null && aggregatedKey != null) {
                if (frequencyFamilyCounter.getDateToFrequencyValueMap().size() == 0)
                    frequencyFamilyCounter.aggregateRecord(DateHelper.format(aggregatedKey.getTimestamp()), 1);
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
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption("ageOffDate", "Frequencies before this date are not aggregated.");
        io.setName("ageOffDate");
        io.setDescription("TransformColumnIterator removes entries with dates that occurred before <ageOffDate>");
        return io;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);
        if (valid) {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                simpleDateFormat.parse(options.get("ageOffDate"));
            } catch (Exception e) {
                valid = false;
            }
        }
        return valid;
    }
    
}

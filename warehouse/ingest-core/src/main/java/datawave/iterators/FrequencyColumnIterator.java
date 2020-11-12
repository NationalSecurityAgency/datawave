package datawave.iterators;

import datawave.data.ColumnFamilyConstants;
import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;

public class FrequencyColumnIterator extends TransformingIterator {
    
    private String ageOffDate = null;
    
    public FrequencyColumnIterator() {}
    
    public FrequencyColumnIterator(FrequencyColumnIterator aThis, IteratorEnvironment environment) {
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
        return PartialKey.ROW_COLFAM;
    }
    
    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator, KVBuffer kvBuffer) throws IOException {
        if (log.isTraceEnabled())
            log.trace("Transforming range for key " + sortedKeyValueIterator.getTopKey().getRow().toString(), new Exception());
        
        Key topKey = null;
        Value topValue = null;
        
        boolean isFrequencyRange = true;
        final Map<String,FrequencyFamilyCounter> frequencyCounterByDatatype = new TreeMap<>();
        
        while (sortedKeyValueIterator.hasTop()) {
            topKey = sortedKeyValueIterator.getTopKey();
            topValue = sortedKeyValueIterator.getTopValue();
            
            // Just do an identity transform if there it is not a Frequency Column
            if (!isFrequencyRange || !topKey.getColumnFamily().equals(ColumnFamilyConstants.COLF_F)) {
                kvBuffer.append(sortedKeyValueIterator.getTopKey(), sortedKeyValueIterator.getTopValue());
                sortedKeyValueIterator.next();
                isFrequencyRange = false;
                continue;
            }
            
            if (MetadataHelper.isAggregatedFreqKey(topKey)) {
                String datatype = MetadataHelper.getDataTypeFromAggregatedFreqCQ(topKey.getColumnQualifier());
                if (!frequencyCounterByDatatype.containsKey(datatype)) {
                    FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter();
                    frequencyFamilyCounter.deserializeCompressedValue(topValue, ageOffDate);
                    frequencyCounterByDatatype.put(datatype, frequencyFamilyCounter);
                } else {
                    FrequencyFamilyCounter previousCounter = frequencyCounterByDatatype.get(datatype);
                    previousCounter.aggregateCompressedValue(topValue, ageOffDate);
                }
            } else {
                String cq = topKey.getColumnQualifier().toString();
                int index = cq.indexOf('\0');
                String date = cq.substring(index + 1);
                if (ageOffDate == null || ageOffDate.compareTo(date) < 0) {
                    String datatype = cq.substring(0, index);
                    FrequencyFamilyCounter frequencyFamilyCounter = frequencyCounterByDatatype.get(datatype);
                    if (frequencyFamilyCounter == null) {
                        frequencyFamilyCounter = new FrequencyFamilyCounter();
                        frequencyCounterByDatatype.put(datatype, frequencyFamilyCounter);
                    }
                    frequencyFamilyCounter.aggregateRecord(date, SummingCombiner.VAR_LEN_ENCODER.decode(topValue.get()).intValue());
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Aged off the date " + date);
                    }
                }
            }
            
            sortedKeyValueIterator.next();
        }
        
        if (!isFrequencyRange)
            return;
        
        for (Map.Entry<String,FrequencyFamilyCounter> entry : frequencyCounterByDatatype.entrySet()) {
            if (!entry.getValue().getDateToFrequencyValueMap().isEmpty()) {
                Key key = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(entry.getKey() + '\0' + MetadataHelper.AGGREGATED_FREQ_COL_QUAL),
                                topKey.getColumnVisibility(), topKey.getTimestamp());
                kvBuffer.append(key, entry.getValue().serialize());
            }
        }
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
            if (options.containsKey("ageOffDate")) {
                try {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                    simpleDateFormat.parse(options.get("ageOffDate"));
                } catch (Exception e) {
                    valid = false;
                }
            }
        }
        return valid;
    }
    
}

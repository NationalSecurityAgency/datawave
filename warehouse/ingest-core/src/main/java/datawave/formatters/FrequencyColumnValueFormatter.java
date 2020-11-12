package datawave.formatters;

import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class FrequencyColumnValueFormatter implements Formatter {
    
    private Iterator<Map.Entry<Key,Value>> iter;
    Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }
    
    @Override
    public String next() {
        StringBuilder sb = new StringBuilder();
        Map.Entry<Key,Value> entry = iter.next();
        
        try {
            
            if (MetadataHelper.isAggregatedFreqKey(entry.getKey())) {
                FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter();
                frequencyFamilyCounter.deserializeCompressedValue(entry.getValue());
                frequencyFamilyCounter
                                .getDateToFrequencyValueMap()
                                .entrySet()
                                .stream()
                                .sorted(Comparator.comparing(Map.Entry::getKey))
                                .forEach(sorted -> sb.append(entry.getKey().getRow()).append(" " + entry.getKey().getColumnFamily().toString())
                                                .append(" " + MetadataHelper.getDataTypeFromAggregatedFreqCQ(entry.getKey().getColumnQualifier()))
                                                .append(" Date: " + sorted.getKey().getYyyymmdd()).append(" Frequency: " + sorted.getValue().getValue() + "\n"));
                return sb.toString();
            }
            
        } catch (Exception e) {
            if (log.isTraceEnabled())
                log.info("Using the default formatter for compressed ");
        }
        
        return DefaultFormatter.formatEntry(entry, false);
        
    }
    
    @Override
    public void remove() {
        iter.remove();
    }
    
    @Override
    public void initialize(Iterable<Map.Entry<Key,Value>> scanner, FormatterConfig formatterConfig) {
        this.iter = scanner.iterator();
    }
    
}

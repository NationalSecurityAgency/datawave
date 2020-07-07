package datawave.formatters;

import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.hadoop.io.Text;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class FrequencyColumnValueFormatter implements Formatter, ScanInterpreter {
    
    private Iterator<Map.Entry<Key,Value>> iter;
    private FormatterConfig config;
    private FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter();
    
    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }
    
    @Override
    public String next() {
        StringBuilder sb = new StringBuilder();
        try {
            Map.Entry<Key,Value> entry = iter.next();
            
            if (entry.getKey().getColumnQualifier().toString().startsWith(MetadataHelper.COL_QUAL_PREFIX))
                frequencyFamilyCounter.deserializeCompressedValue(entry.getValue());
            else
                sb.append(entry.getValue().toString());
            
            frequencyFamilyCounter.getDateToFrequencyValueMap().entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                            .forEach(sorted -> sb.append("Date: " + sorted.getKey() + " Frequency: " + sorted.getValue().toString() + "\n"));
           
        } catch (NullPointerException npe) {
            System.out.println("There was an exception thrown");
            sb.append("There was an error processing");
        }
        /*
         * if (config.willPrintTimestamps()) { sb.append(Long.toString(entry.getKey().getTimestamp())); sb.append("  "); }
         */
        
        return sb.toString();
    }
    
    @Override
    public void remove() {
        iter.remove();
    }
    
    @Override
    public void initialize(Iterable<Map.Entry<Key,Value>> scanner, FormatterConfig formatterConfig) {
        this.iter = scanner.iterator();
        // this.config = new FormatterConfig(config);
    }
    
    private int fromChar(char b) {
        if (b >= '0' && b <= '9') {
            return (b - '0');
        } else if (b >= 'a' && b <= 'f') {
            return (b - 'a' + 10);
        }
        
        throw new IllegalArgumentException("Bad char " + b);
    }
    
    private byte[] toBinary(String hex) {
        hex = hex.replace("-", "");
        
        byte[] bin = new byte[(hex.length() / 2) + (hex.length() % 2)];
        
        int j = 0;
        for (int i = 0; i < bin.length; i++) {
            bin[i] = (byte) (fromChar(hex.charAt(j++)) << 4);
            if (j >= hex.length())
                break;
            bin[i] |= (byte) fromChar(hex.charAt(j++));
        }
        
        return bin;
    }
    
    @Override
    public Text interpretRow(Text row) {
        return new Text(toBinary(row.toString()));
    }
    
    @Override
    public Text interpretBeginRow(Text row) {
        return interpretRow(row);
    }
    
    @Override
    public Text interpretEndRow(Text row) {
        return interpretRow(row);
    }
    
    @Override
    public Text interpretColumnFamily(Text cf) {
        return interpretRow(cf);
    }
    
    @Override
    public Text interpretColumnQualifier(Text cq) {
        return interpretRow(cq);
    }
    
}

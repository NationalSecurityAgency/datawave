package datawave.core.iterators.key;

import org.apache.accumulo.core.data.Key;

import java.util.ArrayList;

/**
 * Utility class that operates on a TermFrequency key's ColumnQualifier
 */
public class TFKey {
    
    // CQ partitions
    private String datatype;
    private String uid;
    private String value;
    private String field;
    private String uidAndValue;
    
    private byte[] backing;
    // null bytes are split points
    private final ArrayList<Integer> splits = new ArrayList<>();
    
    public void parse(Key k) {
        this.datatype = null;
        this.uid = null;
        this.value = null;
        this.field = null;
        this.uidAndValue = null;
        
        this.splits.clear();
        this.backing = k.getColumnQualifierData().getBackingArray();
        
        // Find all possible split points
        for (int i = 0; i < k.getColumnQualifierData().length(); i++) {
            if (backing[i] == '\u0000')
                splits.add(i);
        }
    }
    
    public String getDatatype() {
        if (isValid() && datatype == null && splits.size() > 1) {
            int stop = splits.get(0);
            datatype = new String(backing, 0, stop);
        }
        return datatype;
    }
    
    public String getUid() {
        if (isValid() && uid == null) {
            if (splits.size() == 1) {
                int start = splits.get(0) + 1;
                uid = new String(backing, start, (backing.length - start));
            } else if (splits.size() > 1) {
                int start = splits.get(0) + 1;
                int stop = splits.get(1);
                uid = new String(backing, start, (stop - start));
            }
        }
        return uid;
    }
    
    public String getValue() {
        if (isValid() && value == null && splits.size() >= 3) {
            int start = splits.get(1) + 1;
            int stop = splits.get(splits.size() - 1);
            value = new String(backing, start, (stop - start));
        }
        return value;
    }
    
    public String getField() {
        if (isValid() && field == null && splits.size() >= 3) {
            int start = splits.get(splits.size() - 1) + 1;
            field = new String(backing, start, (backing.length - start));
        }
        return field;
    }
    
    public String getUidAndValue() {
        if (isValid() && uidAndValue == null && getUid() != null && getValue() != null) {
            uidAndValue = getUid() + '\u0000' + getValue();
        }
        return uidAndValue;
    }
    
    public boolean isValid() {
        return backing != null && splits.size() >= 3;
    }
}

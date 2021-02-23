package datawave.core.iterators.key;

import org.apache.accumulo.core.data.ArrayByteSequence;
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
    private ArrayList<Integer> nulls;
    
    public void parse(Key k) {
        this.datatype = null;
        this.uid = null;
        this.value = null;
        this.field = null;
        this.uidAndValue = null;
        
        this.nulls = new ArrayList<>();
        this.backing = k.getColumnQualifierData().getBackingArray();
        
        // Find all possible split points
        for (int i = 0; i < k.getColumnQualifierData().length(); i++) {
            if (backing[i] == '\u0000')
                nulls.add(i);
        }
    }
    
    public String getDatatype() {
        if (datatype == null) {
            if (nulls.size() > 1) {
                int stop = nulls.get(0);
                datatype = new String(backing, 0, stop);
            }
        }
        return datatype;
    }
    
    public String getUid() {
        if (uid == null) {
            if (nulls.size() == 1) {
                int start = nulls.get(0) + 1;
                uid = new String(backing, start, (backing.length - start));
            } else if (nulls.size() > 1) {
                int start = nulls.get(0) + 1;
                int stop = nulls.get(1);
                uid = new String(backing, start, (stop - start));
            }
        }
        return uid;
    }
    
    public String getValue() {
        if (value == null) {
            if (nulls.size() >= 3) {
                int start = nulls.get(1) + 1;
                int stop = nulls.get(nulls.size() - 1);
                value = new String(backing, start, (stop - start));
            }
        }
        return value;
    }
    
    public String getField() {
        if (field == null) {
            if (nulls.size() >= 3) {
                int start = nulls.get(nulls.size() - 1) + 1;
                field = new String(backing, start, (backing.length - start));
            }
        }
        return field;
    }
    
    public String getUidAndValue() {
        if (uidAndValue == null) {
            uidAndValue = getUid() + '\u0000' + getValue();
        }
        return uidAndValue;
    }
}

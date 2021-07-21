package datawave.query.postprocessing.tf;

import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * A field name and value which is sorted on {@code <value>\0<field>}. Only operates on Term Frequency keys.
 */
@Deprecated
public class FieldValue implements Comparable<FieldValue> {
    private int nullOffset;
    private String valueField;
    
    public FieldValue(String field, String value) {
        this.nullOffset = value.length();
        this.valueField = value + '\0' + field;
    }
    
    /**
     * A distance between this field value and another. Here we want a distance that correlates with the number of keys between here and there for the same.
     * Essentially we want the inverse of the number of bytes that match. document.
     *
     * @param fv
     * @return a distance between here and there (negative means there is before here)
     */
    public double distance(FieldValue fv) {
        byte[] s1 = getValueField().getBytes();
        byte[] s2 = fv.getValueField().getBytes();
        int len = Math.min(s1.length, s2.length);
        
        int matches = 0;
        int lastCharDiff = 0;
        
        for (int i = 0; i <= len; i++) {
            lastCharDiff = getValue(s2, i) - getValue(s1, i);
            if (lastCharDiff == 0) {
                matches++;
            } else {
                break;
            }
        }
        
        return Math.copySign(1.0d / (matches + 1), lastCharDiff);
    }
    
    private int getValue(byte[] bytes, int index) {
        if (index >= bytes.length) {
            return 0;
        } else {
            return bytes[index];
        }
    }
    
    public String getValueField() {
        return valueField;
    }
    
    public String getField() {
        return valueField.substring(nullOffset + 1);
    }
    
    public String getValue() {
        return valueField.substring(0, nullOffset);
    }
    
    @Override
    public int compareTo(FieldValue o) {
        return valueField.compareTo(o.valueField);
    }
    
    @Override
    public int hashCode() {
        return valueField.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldValue) {
            return valueField.equals(((FieldValue) obj).valueField);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return getField() + " -> " + getValue();
    }
    
    public static FieldValue getFieldValue(Key key) {
        return getFieldValue(key.getColumnQualifier());
    }
    
    public static FieldValue getFieldValue(Text cqText) {
        if (cqText == null) {
            return null;
        }
        return getFieldValue(cqText.toString());
    }
    
    public static FieldValue getFieldValue(String cq) {
        if (cq == null) {
            return null;
        }
        
        // pull apart the cq
        String[] cqParts = StringUtils.split(cq, '\0');
        
        // if we do not even have the first datatype\0uid, then lets find it
        if (cqParts.length <= 2) {
            return null;
        }
        
        // get the value and field
        String value = "";
        String field = "";
        if (cqParts.length >= 4) {
            field = cqParts[cqParts.length - 1];
            value = cqParts[2];
            // in case the value had null characters therein
            for (int i = 3; i < (cqParts.length - 1); i++) {
                value = value + '\0' + cqParts[i];
            }
        } else if (cqParts.length == 3) {
            value = cqParts[2];
        }
        
        return new FieldValue(field, value);
    }
    
}

package datawave.query.data.parsers;

import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A {@link KeyParser} for FieldIndex keys
 * <p>
 * FieldIndex key structure is
 * <ul>
 * <li>row - the shard</li>
 * <li>column family - "fi\0FIELD"</li>
 * <li>column qualifier - "value\0datatype\0uid"</li>
 * </ul>
 */
public class FieldIndexKey implements KeyParser {
    
    private String field;
    private String value;
    private String datatype;
    private String uid;
    private String rootUid;
    
    private ByteSequence cqBytes;
    
    private int firstNull;
    private int secondNull;
    
    private Key key;
    
    /**
     * Sets the key and resets all supporting objects
     *
     * @param k
     *            a field index key
     */
    public void parse(Key k) {
        clearState();
        this.key = k;
    }
    
    /**
     * Clears existing state
     */
    @Override
    public void clearState() {
        this.field = null;
        this.value = null;
        this.datatype = null;
        this.uid = null;
        this.rootUid = null;
        
        this.cqBytes = null;
        
        this.firstNull = -1;
        this.secondNull = -1;
    }
    
    /**
     * Backwards traversal of the column qualifier to find the two null indices
     */
    private void traverseColumnQualifier() {
        
        if (key == null || (firstNull != -1 && secondNull != -1)) {
            return;
        }
        
        if (cqBytes == null) {
            cqBytes = key.getColumnQualifierData();
        }
        
        for (int i = cqBytes.length() - 1; i >= 0; i--) {
            if (cqBytes.byteAt(i) == 0x00) {
                if (secondNull == -1) {
                    secondNull = i;
                } else {
                    firstNull = i;
                    break;
                }
            }
        }
    }
    
    @Override
    public String getField() {
        if (field == null) {
            if (key != null) {
                ByteSequence backing = key.getColumnFamilyData();
                if (backing.length() > 3) {
                    field = backing.subSequence(3, backing.length()).toString();
                }
            }
            
            if (field == null) {
                throw new IllegalArgumentException("Failed to parse FIELD from fi key");
            }
        }
        return field;
    }
    
    @Override
    public String getValue() {
        if (value == null) {
            if (cqBytes == null) {
                traverseColumnQualifier();
            }
            if (firstNull != -1 && secondNull != -1) {
                value = cqBytes.subSequence(0, firstNull).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse VALUE from fi key");
            }
        }
        return value;
    }
    
    @Override
    public String getDatatype() {
        if (datatype == null) {
            if (cqBytes == null) {
                traverseColumnQualifier();
            }
            if (firstNull != -1 && secondNull != -1) {
                datatype = cqBytes.subSequence(firstNull + 1, secondNull).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse DATATYPE from fi key");
            }
        }
        return datatype;
    }
    
    @Override
    public String getUid() {
        if (uid == null) {
            if (cqBytes == null) {
                traverseColumnQualifier();
            }
            if (firstNull != -1 && secondNull != -1) {
                uid = cqBytes.subSequence(secondNull + 1, cqBytes.length()).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse UID from fi key");
            }
        }
        return uid;
    }
    
    @Override
    public String getRootUid() {
        if (rootUid == null) {
            if (uid == null) {
                getUid();
            }
            
            if (uid == null) {
                throw new IllegalArgumentException("Failed to parse root UID from tf key");
            }
            
            rootUid = TLD.getRootUid(uid);
        }
        return rootUid;
    }
}

package datawave.query.data.parsers;

import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A {@link KeyParser} for TermFrequency keys
 * <p>
 * TermFrequency key structure is
 * <ul>
 * <li>row - the shard</li>
 * <li>column family - "tf"</li>
 * <li>column qualifier - "datatype\0uid\value\0FIELD</li>
 * </ul>
 */
public class TermFrequencyKey implements KeyParser {
    
    // CQ partitions
    private String datatype;
    private String uid;
    private String rootUid;
    private String value;
    private String field;
    private String uidAndValue;
    
    private ByteSequence cq;
    
    private int firstNull;
    private int secondNull;
    private int thirdNull;
    
    private Key key;
    
    @Override
    public void parse(Key k) {
        clearState();
        this.key = k;
    }
    
    @Override
    public void clearState() {
        this.key = null;
        
        this.datatype = null;
        this.uid = null;
        this.rootUid = null;
        this.value = null;
        this.field = null;
        
        this.uidAndValue = null;
        this.cq = null;
        
        this.firstNull = -1;
        this.secondNull = -1;
        this.thirdNull = -1;
    }
    
    /**
     * Iterate to find the split points
     */
    public void traverseColumnQualifier() {
        
        // bail out if the backing key is null, or if any split point was found
        if (key == null || firstNull != -1 || secondNull != -1 || thirdNull != -1) {
            return;
        }
        
        this.cq = key.getColumnQualifierData();
        
        // Find all possible split points
        for (int i = 0; i < cq.length(); i++) {
            if (cq.byteAt(i) == 0x00) {
                if (firstNull == -1) {
                    firstNull = i;
                } else {
                    secondNull = i;
                    break;
                }
            }
        }
        
        // find third null via reverse iteration
        for (int i = cq.length() - 1; i >= 0; i--) {
            if (cq.byteAt(i) == 0x00) {
                thirdNull = i;
                break;
            }
        }
    }
    
    @Override
    public String getDatatype() {
        if (datatype == null) {
            traverseColumnQualifier();
            
            if (firstNull != -1 && cq != null) {
                datatype = cq.subSequence(0, firstNull).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse DATATYPE from tf key");
            }
        }
        return datatype;
    }
    
    @Override
    public String getUid() {
        if (uid == null) {
            traverseColumnQualifier();
            
            if (firstNull != -1 && secondNull != -1 && cq != null) {
                uid = cq.subSequence(firstNull + 1, secondNull).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse UID from tf key");
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
    
    @Override
    public String getValue() {
        if (value == null) {
            traverseColumnQualifier();
            
            if (secondNull != -1 && thirdNull != -1 && cq != null) {
                value = cq.subSequence(secondNull + 1, thirdNull).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse VALUE from tf key");
            }
        }
        return value;
    }
    
    @Override
    public String getField() {
        if (field == null) {
            traverseColumnQualifier();
            
            if (thirdNull != -1 && cq != null) {
                field = cq.subSequence(thirdNull + 1, cq.length()).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse FIELD from tf key");
            }
        }
        return field;
    }
    
    public String getUidAndValue() {
        // call to isValid is made as part of the getUid and getValue calls
        if (uidAndValue == null && getUid() != null && getValue() != null) {
            uidAndValue = getUid() + '\u0000' + getValue();
        }
        return uidAndValue;
    }
    
    /**
     * Get the key
     *
     * @return the key
     */
    @Override
    public Key getKey() {
        return key;
    }
}

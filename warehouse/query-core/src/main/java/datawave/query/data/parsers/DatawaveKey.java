package datawave.query.data.parsers;

import java.util.Arrays;

import datawave.tables.schema.ShardFamilyConstants;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 */
public class DatawaveKey {
    
    /**
     * Null byte
     */
    public static final int NULL = 0x00;
    
    protected Key key;
    protected KeyType myType;
    
    public enum KeyType {
        EVENT, INDEX_EVENT, INDEX, TERM_OFFSETS, OTHER;
    }
    
    protected String fieldName = null;
    protected String fieldValue = null;
    protected String shardId = null;
    protected String dataType = null;
    protected String uid = null;
    
    boolean invalidKey = false;
    protected Text row;
    protected long ts;
    
    /**
     * @param key
     *            the key
     */
    public DatawaveKey(Key key) {
        this.key = key;
        myType = parseKey(key);
    }
    
    public DatawaveKey setReverse(final boolean isReverse) {
        if (isReverse && myType == KeyType.INDEX) {
            // reverse the key so that it's been reversed.
            fieldValue = new StringBuilder(fieldValue).reverse().toString();
        }
        
        return this;
    }
    
    private KeyType parseKey(Key currentKey) {
        
        this.row = currentKey.getRow();
        this.ts = currentKey.getTimestamp();
        // get the column qualifier, so that we can use it throughout
        final byte[] cq = currentKey.getColumnQualifierData().getBackingArray();
        
        int cqLength = cq.length;
        
        int nullIndex = -1;
        
        final byte[] cf = currentKey.getColumnFamilyData().getBackingArray();
        
        int cfLength = cf.length;
        
        // over the course of many evolutions the jit compiler should inline these array checks
        // along with the FI check, below.
        if (Arrays.equals(cf, ShardFamilyConstants.TF_BYTES)) {
            this.shardId = currentKey.getRow().toString();
            return parseTermFrequency(cq);
        } else if (Arrays.equals(cf, ShardFamilyConstants.DOCUMENT_BYTES)) {
            this.shardId = currentKey.getRow().toString();
            return parseDocumentKey(cq);
        } else {
            
            for (int i = 0; i < cfLength - 1; i++) {
                if (cf[i] == NULL) {
                    nullIndex = i;
                    break;
                }
            }
            
            // for the fi, find the last and second to last null byte, then grab that region
            if (WritableComparator.compareBytes(cf, 0, nullIndex, ShardFamilyConstants.FI_BYTES, 0, ShardFamilyConstants.FI_BYTES.length) == 0) {
                // fi column
                this.shardId = currentKey.getRow().toString();
                
                fieldName = new String(cf, nullIndex + 1, cfLength - nullIndex - 1);
                int uidIndex = -1;
                nullIndex = -1;
                
                for (int i = cqLength - 1; i >= 0; i--) {
                    if (cq[i] == NULL) {
                        if (uidIndex == -1)
                            uidIndex = i;
                        else {
                            nullIndex = i + 1;
                        }
                        if (uidIndex > 0 && nullIndex > 0)
                            break;
                    }
                }
                
                if (uidIndex > 0 && nullIndex > 0) {
                    fieldValue = new String(cq, 0, nullIndex - 1);
                    dataType = new String(cq, nullIndex, (uidIndex - nullIndex));
                    uid = new String(cq, uidIndex + 1, cqLength - uidIndex - 1);
                } else {
                    invalidKey = true;
                }
                return KeyType.INDEX_EVENT;
            } else {
                
                // data column
                if (nullIndex > 0) {
                    this.shardId = currentKey.getRow().toString();
                    // we have an event column
                    dataType = new String(cf, 0, nullIndex);
                    uid = new String(cf, nullIndex + 1, cfLength - nullIndex - 1);
                    
                    nullIndex = -1;
                    for (int i = 0; i < cqLength - 1; i++) {
                        if (cq[i] == NULL) {
                            nullIndex = i;
                            break;
                        }
                    }
                    if (nullIndex < 0) {
                        invalidKey = true;
                        fieldName = new String(cq);
                        fieldValue = "";
                    } else {
                        fieldName = new String(cq, 0, nullIndex);
                        fieldValue = new String(cq, nullIndex + 1, cqLength - nullIndex - 1);
                    }
                    
                    return KeyType.EVENT;
                    
                } else {
                    fieldValue = currentKey.getRow().toString();
                    fieldName = currentKey.getColumnFamily().toString();
                    nullIndex = -1;
                    for (int i = 0; i < cqLength - 1; i++) {
                        if (cq[i] == NULL) {
                            nullIndex = i;
                            break;
                        }
                    }
                    if (nullIndex < 0)
                        invalidKey = true;
                    else {
                        shardId = new String(cq, 0, nullIndex);
                        dataType = new String(cq, nullIndex + 1, cqLength - nullIndex - 1);
                    }
                    
                    return KeyType.INDEX;
                }
                
            }
        }
        
    }
    
    private KeyType parseDocumentKey(byte[] cq) {
        int uidIndex = -1;
        int nullIndex = -1;
        int cqLength = cq.length;
        
        for (int i = 0; i < cqLength - 1; i++) {
            if (cq[i] == NULL) {
                if (uidIndex == -1)
                    uidIndex = i;
                else {
                    nullIndex = i;
                }
                if (uidIndex > 0 && nullIndex > 0)
                    break;
            }
        }
        
        if (uidIndex == -1 || nullIndex == -1) {
            invalidKey = true;
        } else {
            dataType = new String(cq, 0, uidIndex);
            uid = new String(cq, uidIndex + 1, (nullIndex - uidIndex - 1));
            fieldName = new String(cq, nullIndex + 1, cq.length - nullIndex - 1);
            
            /**
             * These fields don't exist within document fields.
             */
            fieldValue = "";
            
        }
        
        return KeyType.OTHER;
    }
    
    /**
     * @param cq
     *            column qualifier
     * @return a keytype
     */
    private KeyType parseTermFrequency(byte[] cq) {
        
        int cqLength = cq.length;
        
        int nullIndex = -1;
        
        int uidIndex = -1;
        
        for (int i = 0; i < cqLength - 1; i++) {
            if (cq[i] == NULL) {
                nullIndex = i;
                break;
            }
        }
        
        dataType = new String(cq, 0, nullIndex);
        
        for (int i = nullIndex + 1; i < cqLength - 1; i++) {
            if (cq[i] == NULL) {
                uidIndex = i;
                break;
            }
        }
        
        if (uidIndex > 0) {
            uid = new String(cq, nullIndex + 1, uidIndex - nullIndex - 1);
            
            nullIndex = -1;
            
            for (int i = cqLength - 1; i >= 0; i--) {
                if (cq[i] == NULL) {
                    nullIndex = i;
                    
                    if (uidIndex > 0 && nullIndex > 0)
                        break;
                }
            }
            
            if (uidIndex == -1 || nullIndex == -1 || nullIndex >= cqLength)
                invalidKey = true;
            else {
                fieldValue = new String(cq, uidIndex + 1, nullIndex - uidIndex - 1);
                fieldName = new String(cq, nullIndex + 1, cqLength - nullIndex - 1);
            }
            
        } else
            invalidKey = true;
        
        return KeyType.TERM_OFFSETS;
    }
    
    public String getShardId(boolean ignore) {
        if (!ignore && invalidKey)
            throw new RuntimeException("Attempting to access invalid key part");
        else
            return shardId;
    }
    
    public String getShardId() {
        return getShardId(true);
    }
    
    public String getFieldName(boolean ignore) {
        if (!ignore && invalidKey)
            throw new RuntimeException("Attempting to access invalid key part");
        else
            return fieldName;
    }
    
    public String getFieldName() {
        return getFieldName(true);
    }
    
    public String getFieldValue(boolean ignore) {
        if (!ignore && invalidKey)
            throw new RuntimeException("Attempting to access invalid key part");
        else
            return fieldValue;
    }
    
    public String getFieldValue() {
        return getFieldValue(true);
    }
    
    public KeyType getType() {
        return myType;
    }
    
    public String getUid() {
        return uid;
    }
    
    public Text getRow() {
        return row;
    }
    
    public long getTimeStamp() {
        return ts;
    }
    
    public String getDataType() {
        if (invalidKey)
            throw new RuntimeException("Attempting to access invalid key part of " + key);
        else
            return new String(dataType);
    }
    
    public boolean isInvalidKey() {
        return invalidKey;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        
        builder.append(fieldName).append(" ").append(fieldValue).append(" ");
        return builder.toString();
    }
}

package datawave.query.data.parsers;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

/**
 * A {@link KeyParser} for Event keys
 * <p>
 * EventKey structure is
 * <ul>
 * <li>row - the shard</li>
 * <li>column family - "datatype\0uid"</li>
 * <li>column qualifier - "FIELD\0value"</li>
 * </ul>
 */
public class EventKey implements KeyParser {
    
    private Key key;
    
    private ByteSequence cf;
    private ByteSequence cq;
    
    private int cfSplit;
    private int cqSplit;
    private int rootUidStop;
    
    private String datatype;
    private String uid;
    private String rootUid;
    private String value;
    private String field;
    
    @Override
    public void parse(Key k) {
        clearState();
        this.key = k;
    }
    
    public void clearState() {
        this.cf = null;
        this.cq = null;
        
        this.cfSplit = -1;
        this.cqSplit = -1;
        this.rootUidStop = -1;
        
        this.datatype = null;
        this.uid = null;
        this.rootUid = null;
        this.value = null;
        this.field = null;
    }
    
    /**
     * Helper method that scans the column family to find the split points
     */
    private void scanColumnFamily() {
        
        if (key == null) {
            return;
        }
        
        this.cf = key.getColumnFamilyData();
        for (int i = 0; i < cf.length(); i++) {
            if (cf.byteAt(i) == 0x00) {
                cfSplit = i;
                break;
            }
        }
    }
    
    /**
     * Helper method that scans the column qualifier to find the split points
     */
    private void scanColumnQualifier() {
        if (key == null) {
            return;
        }
        
        cq = key.getColumnQualifierData();
        for (int i = 0; i < cq.length(); i++) {
            if (cq.byteAt(i) == 0x00) {
                cqSplit = i;
                break;
            }
        }
    }
    
    @Override
    public String getDatatype() {
        if (datatype == null) {
            if (cf == null) {
                scanColumnFamily();
            }
            if (cfSplit != -1) {
                datatype = cf.subSequence(0, cfSplit).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse DATATYPE from event key");
            }
        }
        return datatype;
    }
    
    @Override
    public String getUid() {
        if (uid == null) {
            if (cf == null) {
                scanColumnFamily();
            }
            if (cfSplit != -1) {
                uid = cf.subSequence(cfSplit + 1, cf.length()).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse UID from event key");
            }
        }
        return uid;
    }
    
    @Override
    public String getRootUid() {
        if (rootUid == null) {
            
            if (cfSplit == -1) {
                scanColumnFamily();
            }
            
            if (rootUidStop == -1) {
                scanUid();
            }
            
            if (rootUidStop == -1) {
                rootUid = uid;
            } else {
                rootUid = cf.subSequence(cfSplit + 1, rootUidStop).toString();
            }
            
            if (rootUid == null) {
                throw new IllegalArgumentException("Failed to parse root uid from event key");
            }
        }
        return rootUid;
    }
    
    /**
     * Performs an optimized scan of the uid.
     * <p>
     * A HashUID is minimally a 21 character string of three alphanumerics of length six, joined by two dots
     */
    private void scanUid() {
        if (key == null) {
            return;
        }
        
        // given that we're working with a HashUID we could probably optimize the start
        // three parts of length 6 and two dots gives us a minimum start of 21
        for (int i = (cfSplit + 21); i < cf.length(); i++) {
            if (cf.byteAt(i) == '.') {
                rootUidStop = i;
                break;
            }
        }
    }
    
    @Override
    public String getValue() {
        if (value == null) {
            if (cq == null) {
                scanColumnQualifier();
            }
            if (cqSplit != -1) {
                value = cq.subSequence(cqSplit + 1, cq.length()).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse VALUE from event key");
            }
        }
        return value;
    }
    
    @Override
    public String getField() {
        if (field == null) {
            if (cq == null) {
                scanColumnQualifier();
            }
            if (cqSplit != -1) {
                field = cq.subSequence(0, cqSplit).toString();
            } else {
                throw new IllegalArgumentException("Failed to parse FIELD from event key");
            }
        }
        return field;
    }
    
}

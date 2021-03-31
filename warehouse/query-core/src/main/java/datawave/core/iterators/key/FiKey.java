package datawave.core.iterators.key;

import org.apache.hadoop.io.Text;

/**
 * Class that lazily parses the elements from a field index key's column qualifier, with some methods to support seeking.
 *
 * FI key structure like shard:fi\0FIELD:value\0datatype\0uid
 */
public class FiKey {
    
    // the value\0datatype\0uid
    private String cq;
    
    private String value;
    private String datatype;
    private String uid;
    
    // index of the nulls for easier parsing
    private int lastNull = 0;
    private int firstNull = 0;
    
    public void parse(Text columnQualifier) {
        this.cq = columnQualifier.toString();
        // Avoid values with nulls by traversing backwards
        this.lastNull = cq.lastIndexOf('\u0000');
        this.firstNull = cq.lastIndexOf('\u0000', lastNull - 1);
        this.value = null;
        this.datatype = null;
        this.uid = null;
    }
    
    public String getValue() {
        if (value == null) {
            value = cq.substring(0, firstNull);
        }
        return value;
    }
    
    public String getDatatype() {
        if (datatype == null) {
            datatype = cq.substring(firstNull + 1, lastNull);
        }
        return datatype;
    }
    
    public String getUid() {
        if (uid == null) {
            uid = cq.substring(lastNull + 1);
        }
        return uid;
    }
    
    public boolean uidStartsWith(String uid) {
        return getUid().startsWith(uid);
    }
}

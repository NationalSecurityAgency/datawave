package nsa.datawave.query.rewrite.predicate;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import nsa.datawave.query.rewrite.Constants;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query except for the root document in which case all fields
 * are returned.
 */
public class TLDEventDataFilter extends EventDataQueryFilter {
    
    public static final byte[] FI_CF = new Text("fi").getBytes();
    public static final byte[] TF_CF = Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes();
    
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public TLDEventDataFilter(ASTJexlScript script) {
        super(script);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.function.Filter#accept(org.apache.accumulo .core.data.Key)
     */
    @Override
    public boolean apply(Entry<Key,String> input) {
        // if a TLD, then accept em all, other wise defer to the query field
        // filter
        if (isRootPointer(input.getKey())) {
            return true;
        } else {
            return super.apply(input);
        }
    }
    
    /**
     * Define the end key given the from condition.
     * 
     * @param from
     * @return
     */
    @Override
    public Key getStopKey(Key from) {
        return new Key(from.getRow().toString(), from.getColumnFamily().toString() + '\uffff');
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.function.Filter#keep(org.apache.accumulo.core .data.Key)
     */
    @Override
    public boolean keep(Key k) {
        // only keep the data from the top level document
        return isRootPointer(k);
    }
    
    protected String getUid(Key k) {
        String uid;
        String cf = k.getColumnFamily().toString();
        if (cf.equals(Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString())) {
            String cq = k.getColumnQualifier().toString();
            int start = cq.indexOf('\0') + 1;
            uid = cq.substring(start, cq.indexOf('\0', start));
        } else if (cf.startsWith("fi\0")) {
            String cq = k.getColumnQualifier().toString();
            uid = cq.substring(cq.lastIndexOf('\0') + 1);
        } else {
            uid = cf.substring(cf.lastIndexOf('\0') + 1);
        }
        return uid;
    }
    
    protected boolean isRootPointer(Key k) {
        ByteSequence cf = k.getColumnFamilyData();
        
        if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();
            int i = seq.length() - 19;
            for (; i >= 0; i--) {
                
                if (seq.byteAt(i) == '.') {
                    return false;
                } else if (seq.byteAt(i) == 0x00) {
                    break;
                }
            }
            
            for (i += 20; i < seq.length(); i++) {
                if (seq.byteAt(i) == '.') {
                    return false;
                }
            }
            return true;
            
        } else if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();
            int i = 3;
            for (; i < seq.length(); i++) {
                if (seq.byteAt(i) == 0x00) {
                    break;
                }
            }
            
            for (i += 20; i < seq.length(); i++) {
                if (seq.byteAt(i) == '.') {
                    return false;
                } else if (seq.byteAt(i) == 0x00) {
                    return true;
                }
            }
            
            return true;
            
        } else {
            int i = 0;
            for (i = 0; i < cf.length(); i++) {
                
                if (cf.byteAt(i) == 0x00) {
                    break;
                }
            }
            
            for (i += 20; i < cf.length(); i++) {
                
                if (cf.byteAt(i) == '.') {
                    return false;
                } else if (cf.byteAt(i) == 0x00) {
                    return true;
                }
            }
            return true;
        }
        
    }
}

package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

/**
 * Similar to {@link DocumentRangeProvider}, but the end key is built with the maximum value instead of a null byte
 */
public class TLDRangeProvider extends DocumentRangeProvider {
    
    /**
     * Get the stop key for the TLD document range by appending the max value
     *
     * @param k
     *            an initial key
     * @return the stop key
     */
    @Override
    public Key getStopKey(Key k) {
        Text cf = new Text(k.getColumnFamily().toString() + '\uffff');
        return new Key(k.getRow(), cf);
    }
}

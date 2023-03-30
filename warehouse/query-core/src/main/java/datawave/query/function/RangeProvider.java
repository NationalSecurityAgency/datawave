package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * Called by {@link KeyToDocumentData}, range is used to aggregate a document
 */
public interface RangeProvider {
    
    /**
     * Get the start key for the range
     * 
     * @param k
     *            an initial key
     * @return the start key
     */
    Key getStartKey(Key k);
    
    /**
     * Get the stop key for the range
     * 
     * @param k
     *            an initial key
     * @return the stop key
     */
    Key getStopKey(Key k);
    
    /**
     * Get the range given an initial key
     *
     * @param k
     *            an initial key
     * @return the range
     */
    Range getRange(Key k);
}

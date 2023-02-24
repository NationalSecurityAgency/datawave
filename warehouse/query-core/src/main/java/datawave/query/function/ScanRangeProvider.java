package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * Given a key, provides a scan range
 */
public interface ScanRangeProvider {
    
    /**
     * Get the start key for the scan range
     * 
     * @param k
     *            an initial key
     * @return the start key
     */
    Key getStartKey(Key k);
    
    /**
     * Get the stop key for the scan range
     * 
     * @param k
     *            an initial key
     * @return the stop key
     */
    Key getStopKey(Key k);
    
    /**
     * Get the scan range given an initial key
     *
     * @param k
     *            an initial key
     * @return the scan range
     */
    Range getScanRange(Key k);
}

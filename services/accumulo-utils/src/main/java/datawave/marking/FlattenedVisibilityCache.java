package datawave.marking;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * This is a cache that can be used per process to save flattened visibility calculations.
 *
 */
public class FlattenedVisibilityCache {
    private static Map<ColumnVisibility,byte[]> flattenedVisCache = Collections.synchronizedMap(new HashMap<>());
    
    /**
     * Create a flattened visibility, using the cache if possible
     *
     * @param vis
     *            the visibility to flatten
     * @return the flattened visibility
     */
    public static byte[] flatten(ColumnVisibility vis) {
        byte[] visBytes = flattenedVisCache.get(vis);
        if (visBytes == null) {
            visBytes = vis.flatten();
            flattenedVisCache.put(vis, visBytes);
        }
        return visBytes;
    }
    
    public static byte[] flatten(ByteSequence bytes) {
        return flatten(ColumnVisibilityCache.get(bytes));
    }
    
    public static boolean equals(ColumnVisibility left, ColumnVisibility right) {
        return Arrays.equals(flatten(left), flatten(right));
    }
}

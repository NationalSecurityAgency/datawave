package datawave.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple utility class for counting occurrences of keys. Replacement for non-public org.apache.accumulo.core.util.MapCounter.
 *
 * @param <K>
 *            the type of keys maintained by this counter
 */
public class MapCounter<K> {

    private final Map<K,Long> counts = new HashMap<>();

    /**
     * Increment the count for the given key.
     *
     * @param key
     *            the key to increment
     * @param amount
     *            the amount to increment by
     * @return the new count for the key
     */
    public long increment(K key, long amount) {
        long newValue = counts.getOrDefault(key, 0L) + amount;
        counts.put(key, newValue);
        return newValue;
    }

    /**
     * Get the count for the given key.
     *
     * @param key
     *            the key to look up
     * @return the count for the key, or 0 if not present
     */
    public long get(K key) {
        return counts.getOrDefault(key, 0L);
    }

    /**
     * Get the set of keys in this counter.
     *
     * @return the set of keys
     */
    public Set<K> keySet() {
        return counts.keySet();
    }
}

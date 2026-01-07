package datawave.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple utility class for counting occurrences of keys. Replaces the non-public org.apache.accumulo.core.util.MapCounter.
 *
 * @param <K>
 *            the key type
 */
public class MapCounter<K> {
    private final Map<K,Long> map = new HashMap<>();

    /**
     * Increment the count for the given key by the specified amount.
     *
     * @param key
     *            the key to increment
     * @param amount
     *            the amount to add
     * @return the new count
     */
    public long increment(K key, long amount) {
        Long count = map.get(key);
        if (count == null) {
            count = 0L;
        }
        count += amount;
        map.put(key, count);
        return count;
    }

    /**
     * Get the count for the given key.
     *
     * @param key
     *            the key
     * @return the count, or 0 if the key is not present
     */
    public long get(K key) {
        Long count = map.get(key);
        return count == null ? 0L : count;
    }

    /**
     * Get the set of keys.
     *
     * @return the key set
     */
    public Set<K> keySet() {
        return map.keySet();
    }
}

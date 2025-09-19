package datawave.query.function;

import java.util.HashMap;

/**
 * A {@link HashMap} implementation that:
 * <ul>
 * <li>Assumes a default value of 0 for missing keys.</li>
 * <li>Automatically removes any mappings when the value has been updated to a value of 0 or less.</li>
 * </ul>
 */
public class CountMap extends HashMap<String,Integer> {

    private static final long serialVersionUID = -736880224574416162L;

    private static final Integer ZERO = 0;

    /**
     * Increment the value mapped to the given key by 1. If no mapping exists, one will be created with a value of 1.
     *
     * @param key
     *            the key
     * @return the previous value associated with the key, or 0 if no mapping existed
     */
    public Integer increment(String key) {
        return modifyValue(key, 1);
    }

    /**
     * Decrement the value mapped to the given key by 1. If the resulting value is 0, the mapping will be removed from this map.
     *
     * @param key
     *            the key
     * @return the previous value associated with the key, or 0 if no mapping existed
     */
    public Integer decrement(String key) {
        return modifyValue(key, -1);
    }

    /**
     * Modify and update the value mapped to the given key by the given modifier. If a mapping does not exist, one will be created. If the resulting value is
     * zero or less, the mapping for the key will be removed from this map.
     *
     * @param key
     *            the key
     * @param modifier
     *            the modifier
     * @return the previous value associated with the key, or 0 if no mapping existed
     */
    public Integer modifyValue(String key, int modifier) {
        Integer value = get(key);
        value += modifier;
        return put(key, value);
    }

    /**
     * Return the value associated with the given key. If no mapping was found for the key, a default of 0 will be returned.
     *
     * @param key
     *            the key
     * @return the value associated with the key, or 0 if no mapping was found for the key
     */
    @Override
    public Integer get(Object key) {
        return getOrDefault(key, ZERO);
    }

    /**
     * Associate the given value with the given key if and only if the value is greater than zero. If the value is zero or less, the mapping for the key will be
     * removed from this map, and the previous associated value for the key will be returned.
     *
     * @param key
     *            the key
     * @param value
     *            the value
     * @return the previous value associated with the key
     */
    @Override
    public Integer put(String key, Integer value) {
        if (value > 0) {
            return super.put(key, value);
        } else {
            value = get(key);
            remove(key);
            return value;
        }
    }
}

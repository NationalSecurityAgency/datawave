package datawave.ingest.data.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.TypeRegistry;

/**
 * A per-key lookup cache backed by a plain {@link HashMap}, with an optional upper bound on the number of entries it may hold and a configurable policy for
 * what a lookup does once that bound is reached.
 * <p>
 * Unbounded by default -- the historical behavior. The two call sites that use this class, {@code BaseIngestHelper}'s resolved field type cache and
 * {@code XMLFieldConfigHelper}'s resolved field cache, already grew their backing map by one entry per distinct field name (including pattern matches) before
 * this class existed; the entries are tiny, and the mapper JVMs that hold them are short-lived, so unbounded growth was never a practical problem. Configure a
 * {@code max.size} only for a datatype with a genuinely unbounded field vocabulary, where capping memory matters more than the cost of the misses a bound
 * introduces.
 * <p>
 * At the bound, {@link OverflowPolicy#BYPASS} resolves a miss without storing it, so the cache never grows past its bound and the set of cached keys is frozen
 * at whichever ones were seen first; {@link OverflowPolicy#CLEAR} clears the whole cache and starts over, trading periodic cold restarts for keeping whichever
 * keys are currently hot rather than a possibly stale first-seen set.
 * <p>
 * Not thread-safe, matching the {@link HashMap} it wraps: instances are confined to a single thread.
 *
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 */
public final class FieldLookupCache<K,V> {

    /** No bound: the cache grows without limit. The default, and the historical behavior. */
    public static final int UNBOUNDED = Integer.MAX_VALUE;

    /** Suffix appended to a settings prefix to name the property that carries the maximum cache size. */
    public static final String MAX_SIZE_SUFFIX = ".max.size";

    /** Suffix appended to a settings prefix to name the property that carries the overflow policy. */
    public static final String OVERFLOW_POLICY_SUFFIX = ".overflow.policy";

    private static final OverflowPolicy DEFAULT_OVERFLOW_POLICY = OverflowPolicy.BYPASS;

    /** What a lookup does once the cache already holds {@code maxSize} entries and the key it was given is not one of them. */
    public enum OverflowPolicy {
        /** Resolve the value but do not store it, so the cache never grows past its bound and its first-seen keys stay cached. */
        BYPASS,
        /** Clear the cache and store only the new entry, trading periodic full misses for keeping whichever keys are currently hot. */
        CLEAR
    }

    private final HashMap<K,V> map = new HashMap<>();
    private final int maxSize;
    private final OverflowPolicy overflowPolicy;

    /**
     * Create an unbounded cache -- the historical behavior, and the default when nothing is configured.
     */
    public FieldLookupCache() {
        this.maxSize = UNBOUNDED;
        this.overflowPolicy = DEFAULT_OVERFLOW_POLICY;
    }

    /**
     * Create a bounded cache.
     *
     * @param maxSize
     *            the maximum number of entries the cache may hold; must be positive
     * @param overflowPolicy
     *            what a lookup does once the cache is full
     * @throws IllegalArgumentException
     *             if {@code maxSize} is not positive
     */
    public FieldLookupCache(int maxSize, OverflowPolicy overflowPolicy) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero, but was: " + maxSize);
        }
        this.maxSize = maxSize;
        this.overflowPolicy = overflowPolicy;
    }

    /**
     * Return the memoized value for the key, computing and storing it first if this is the first time the key has been seen. A hit costs a single
     * {@link HashMap#get}. A miss that would grow the cache past its bound is handled according to the configured {@link OverflowPolicy}.
     *
     * @param key
     *            the key
     * @param fn
     *            the function used to compute a value when the key is not already cached; must never return {@code null}
     * @return the cached or newly computed value
     */
    public V computeIfAbsent(K key, Function<? super K,? extends V> fn) {
        V value = map.get(key);
        if (value != null) {
            return value;
        }

        value = fn.apply(key);

        if (map.size() >= maxSize) {
            if (overflowPolicy == OverflowPolicy.BYPASS) {
                return value;
            }
            map.clear();
        }

        map.put(key, value);
        return value;
    }

    /**
     * Remove every cached entry.
     */
    public void clear() {
        map.clear();
    }

    /**
     * The number of entries currently cached.
     *
     * @return the current size
     */
    public int size() {
        return map.size();
    }

    /**
     * The configured maximum number of entries.
     *
     * @return the maximum size, or {@link #UNBOUNDED}
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * The configured overflow policy.
     *
     * @return the overflow policy
     */
    public OverflowPolicy getOverflowPolicy() {
        return overflowPolicy;
    }

    /**
     * An unmodifiable view of the cached entries, for tests to inspect.
     *
     * @return an unmodifiable view of the backing map
     */
    Map<K,V> asMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Return the property that supplies this setting: the datatype specific one when it is set, otherwise the {@code all} one.
     *
     * @param conf
     *            the configuration
     * @param typeName
     *            the datatype name
     * @param suffix
     *            the property suffix, which includes its leading dot
     * @return the property name
     */
    private static String propertyFor(Configuration conf, String typeName, String suffix) {
        String typeProperty = typeName + suffix;
        return conf.get(typeProperty) != null ? typeProperty : TypeRegistry.ALL_PREFIX + suffix;
    }

    /**
     * Parse the cache described for a datatype: {@code <typeName><prefix>.max.size} and {@code <typeName><prefix>.overflow.policy}, each falling back to
     * {@code all<prefix>...} when the datatype declares none of its own. When neither the datatype nor {@code all} sets a size, the returned cache is unbounded
     * and the overflow policy is not read, since it has nothing to act on. When only a size is set, the overflow policy defaults to
     * {@link OverflowPolicy#BYPASS}.
     *
     * @param conf
     *            the configuration
     * @param typeName
     *            the datatype name
     * @param prefix
     *            the settings prefix, which includes its leading dot
     * @param <K>
     *            the key type
     * @param <V>
     *            the value type
     * @return a new, empty cache built from the parsed settings
     */
    public static <K,V> FieldLookupCache<K,V> parse(Configuration conf, String typeName, String prefix) {
        String maxSizeProperty = propertyFor(conf, typeName, prefix + MAX_SIZE_SUFFIX);
        String maxSizeValue = conf.get(maxSizeProperty);

        if (maxSizeValue == null) {
            return new FieldLookupCache<>();
        }

        int maxSize;
        try {
            maxSize = Integer.parseInt(maxSizeValue.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(maxSizeProperty + " must be an integer, but was: " + maxSizeValue, e);
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException(maxSizeProperty + " must be greater than zero, but was: " + maxSizeValue);
        }

        String overflowPolicyProperty = propertyFor(conf, typeName, prefix + OVERFLOW_POLICY_SUFFIX);
        String overflowPolicyValue = conf.get(overflowPolicyProperty);

        OverflowPolicy overflowPolicy = DEFAULT_OVERFLOW_POLICY;
        if (overflowPolicyValue != null) {
            try {
                overflowPolicy = OverflowPolicy.valueOf(overflowPolicyValue.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                                overflowPolicyProperty + " must be one of " + Arrays.toString(OverflowPolicy.values()) + ", but was: " + overflowPolicyValue,
                                e);
            }
        }

        return new FieldLookupCache<>(maxSize, overflowPolicy);
    }
}

package datawave.accumulo.inmemory;

import java.util.HashMap;
import java.util.Map;

/**
 * A shared, JVM-wide cache of {@link InMemoryAccumulo} instances keyed by instance name.
 * <p>
 * Every caller that asks for the same name gets the same instance, so separate clients (for example a mock bulk ingest job and the code that reads its results)
 * see the same tables and data. Instances stay in the cache until they are removed with {@link #remove(String)} or {@link #clear()}, so tests that use named
 * instances should clean up after themselves.
 */
public final class InMemoryAccumuloCache {

    private static final Map<String,InMemoryAccumulo> instances = new HashMap<>();

    private InMemoryAccumuloCache() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Gets the InMemoryAccumulo instance with the given name from the shared cache, creating and caching a new instance if there is none.
     *
     * @param instanceName
     *            the instance name
     * @return the cached InMemoryAccumulo for the given name
     */
    public static synchronized InMemoryAccumulo getOrCreate(String instanceName) {
        return instances.computeIfAbsent(instanceName, InMemoryAccumulo::new);
    }

    /**
     * Removes the instance with the given name from the shared cache.
     *
     * @param instanceName
     *            the instance name
     * @return the removed instance, or null if there was none
     */
    public static synchronized InMemoryAccumulo remove(String instanceName) {
        return instances.remove(instanceName);
    }

    /**
     * Removes every instance from the shared cache.
     */
    public static synchronized void clear() {
        instances.clear();
    }

    /**
     * @return the number of cached instances
     */
    public static synchronized int size() {
        return instances.size();
    }
}

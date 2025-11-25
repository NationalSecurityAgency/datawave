package datawave.core.cache;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.map.LRUMap;

/**
 * A {@link LRUMap} implementation of a {@link ClassCache}
 */
public class LRUMapClassCache implements ClassCache {

    private final Map<String,Class<?>> cache;

    public LRUMapClassCache() {
        this(DEFAULT_SIZE);
    }

    public LRUMapClassCache(int size) {
        cache = Collections.synchronizedMap(new LRUMap<>(size));
    }

    @Override
    public String name() {
        return "LRUMap";
    }

    @Override
    public Class<?> get(String name) throws ClassNotFoundException {
        Class<?> clazz = cache.get(name);
        if (clazz == null) {
            clazz = Class.forName(name);
            cache.put(name, clazz);
        }
        return clazz;
    }
}

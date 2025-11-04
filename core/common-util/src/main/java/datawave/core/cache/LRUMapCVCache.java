package datawave.core.cache;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

/**
 * A {@link LRUMap} implementation of a {@link CVCache}
 */
public class LRUMapCVCache implements CVCache {

    private final Map<ByteSequence,ColumnVisibility> cache;

    public LRUMapCVCache() {
        this(DEFAULT_SIZE);
    }

    public LRUMapCVCache(int size) {
        cache = Collections.synchronizedMap(new LRUMap<>(size));
    }

    @Override
    public String name() {
        return "LRUMap";
    }

    @Override
    public ColumnVisibility get(ByteSequence bytes) {
        ColumnVisibility vis = cache.get(bytes);
        if (vis == null) {
            vis = new ColumnVisibility(bytes.toArray());
            cache.put(bytes, vis);
        }
        return vis;
    }
}

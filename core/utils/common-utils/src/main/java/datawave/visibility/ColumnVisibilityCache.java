package datawave.visibility;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

/**
 * A per-process LRU cache of {@link ColumnVisibility} instances keyed by their raw visibility bytes.
 */
public final class ColumnVisibilityCache {
    private static final Map<ByteSequence,ColumnVisibility> cache = Collections.synchronizedMap(new LRUMap<>(256));

    private ColumnVisibilityCache() {}

    public static ColumnVisibility get(ByteSequence bytes) {
        ColumnVisibility vis = cache.get(bytes);
        if (vis == null) {
            vis = new ColumnVisibility(bytes.toArray());
            cache.put(bytes, vis);
        }
        return vis;
    }
}

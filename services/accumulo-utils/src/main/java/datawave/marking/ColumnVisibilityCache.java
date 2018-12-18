package datawave.marking;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.map.LRUMap;

public class ColumnVisibilityCache {
    @SuppressWarnings("unchecked")
    private static Map<ByteSequence,ColumnVisibility> cache = Collections.synchronizedMap(new LRUMap(256));
    
    public static ColumnVisibility get(ByteSequence bytes) {
        ColumnVisibility vis = cache.get(bytes);
        if (vis == null) {
            vis = new ColumnVisibility(bytes.toArray());
            cache.put(bytes, vis);
        }
        return vis;
    }
}

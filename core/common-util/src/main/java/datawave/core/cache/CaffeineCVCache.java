package datawave.core.cache;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * A {@link Caffeine} implementation of a {@link CVCache}
 */
public class CaffeineCVCache implements CVCache {

    private final LoadingCache<ByteSequence,ColumnVisibility> cache;

    public CaffeineCVCache() {
        this(DEFAULT_SIZE);
    }

    public CaffeineCVCache(int size) {
        //  @formatter:off
        cache = Caffeine.newBuilder()
                .maximumSize(size)
                .expireAfterAccess(1, TimeUnit.HOURS)
                .build(bytes -> new ColumnVisibility(bytes.toArray()));
        //  @formatter:on
    }

    @Override
    public String name() {
        return "Caffeine";
    }

    @Override
    public ColumnVisibility get(ByteSequence bytes) {
        return cache.get(bytes);
    }
}

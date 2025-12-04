package datawave.core.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A guava {@link LoadingCache} implementation of a {@link CVCache}
 */
public class GuavaCVCache implements CVCache {

    private final LoadingCache<ByteSequence,ColumnVisibility> cache;

    public GuavaCVCache() {
        this(DEFAULT_SIZE);
    }

    public GuavaCVCache(int size) {
        //  @formatter:off
        cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<>() {
                    @Override
                    public ColumnVisibility load(ByteSequence bytes) {
                        return new ColumnVisibility(bytes.toArray());
                    }
                });
        //  @formatter:on
    }

    @Override
    public String name() {
        return "Guava";
    }

    @Override
    public ColumnVisibility get(ByteSequence bytes) {
        try {
            return cache.get(bytes);
        } catch (ExecutionException e) {
            throw new RuntimeException("Invalid ColumnVisibility", e);
        }
    }
}

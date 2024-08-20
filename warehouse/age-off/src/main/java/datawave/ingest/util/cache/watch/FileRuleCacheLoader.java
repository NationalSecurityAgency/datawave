package datawave.ingest.util.cache.watch;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Cache loader implementation for loading {@link FileRuleCacheValue} referencing {@link Path} keys.
 */
public class FileRuleCacheLoader extends CacheLoader<String,FileRuleCacheValue> {
    private final static int CONFIGURED_DIFF = 1;

    /**
     * Reloads a new {@link FileRuleCacheValue} if the cached value has changes, otherwise returns the @param oldValue.
     *
     * @param key
     *            the key to reload for
     * @param oldValue
     *            the existing value
     * @return a new value if there are changes, otherwise @param oldValue is returned
     * @throws IOException
     *             if any errors occur when loading a new instance of the cache value
     */
    @Override
    public ListenableFuture<FileRuleCacheValue> reload(String key, FileRuleCacheValue oldValue) throws IOException {
        // checks here are performed on the caller thread
        FileRuleCacheValue resultValue = oldValue.hasChanges() ? load(key) : oldValue;
        return Futures.immediateFuture(resultValue);
    }

    /**
     * Loads a new rule cache value instance
     *
     * @param key
     *            the non-null key whose value should be loaded
     * @return a new rule cache value instance
     * @throws IOException
     *             if any errors occur when loading a new instance of the cache value
     */
    @Override
    public FileRuleCacheValue load(String key) throws IOException {
        return FileRuleCacheValue.newCacheValue(key, CONFIGURED_DIFF);
    }
}

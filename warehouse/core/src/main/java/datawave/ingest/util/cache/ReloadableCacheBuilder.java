package datawave.ingest.util.cache;

import java.util.concurrent.Executors;

import datawave.ingest.util.cache.watch.Reloadable;

import java.util.Collection;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Purpose: Enables reloadable caches.
 *
 * Description: Keys consist of objects that are Reloadable, which identify if their underlying value has changed and enable it to reload the value
 *
 * Justification: Based upon Loader, which is further based upon CacheLoader. This enables us to have customized techniques for loading GUAVA caches, and
 * reloading this elements when necessary
 */
public class ReloadableCacheBuilder<K extends Reloadable,V> extends Loader<K,V> {

    private static final Logger log = Logger.getLogger(ReloadableCacheBuilder.class);

    public ReloadableCacheBuilder() {

        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    public ReloadableCacheBuilder(boolean lazy) {
        super(lazy);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.Loader#merge(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void merge(K key, V value) throws Exception {
        // not necessary
    }

    @Override
    public ListenableFuture<V> reload(K key, V oldValue) {

        if (key.hasChanged()) {
            try {
                build(key);
            } catch (Exception e) {
                log.error(e);
            }
        }

        return super.reload(key, oldValue);

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.Loader#build(java.lang.Object)
     */
    @Override
    protected void build(K key) throws Exception {

        if (null == key) {
            if (log.isTraceEnabled())
                log.trace("Rebuild all");
            Collection<K> watchers = entryCache.keySet();
            for (K keyWatcher : watchers) {
                Reloadable watcher = Reloadable.class.cast(keyWatcher);

                if (log.isTraceEnabled())
                    log.trace("rebuild " + watcher + " ? " + watcher.hasChanged());
                if (watcher.hasChanged()) {
                    synchronized (entryCache) {
                        if (log.isTraceEnabled())
                            log.trace("rebuild " + watcher + " ? " + watcher.hasChanged() + " " + watcher.reload());

                        entryCache.put(keyWatcher, (V) watcher.reload());

                    }
                }
            }
        } else {
            Reloadable watcher = Reloadable.class.cast(key);
            synchronized (entryCache) {
                entryCache.put(key, (V) watcher.reload());
            }
        }
    }

    @Override
    public V load(K key) throws Exception {
        V watchVariable = entryCache.get(key);

        if (watchVariable == null) {
            build(key);
            watchVariable = entryCache.get(key);

        }
        return watchVariable;
    }

}

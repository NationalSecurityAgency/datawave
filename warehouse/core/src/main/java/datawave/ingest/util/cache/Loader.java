package datawave.ingest.util.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Description: Base Loader mechanism that allows us a way to reload the cache
 */
public abstract class Loader<K,V> extends CacheLoader<K,V> implements Runnable {
    /**
     * Base cache for all loaders.
     */
    protected Map<K,V> entryCache;

    /**
     * Executor service, instantiated by the super class
     */
    protected ListeningExecutorService executor;

    /**
     * Children of the loader
     */
    protected List<Loader<K,V>> children;

    /**
     * child hash
     */
    protected int childHash = 0;

    protected boolean lazy = true;

    private static final Logger log = Logger.getLogger(Loader.class);

    protected Loader() {
        this(true);

    }

    protected Loader(boolean lazy) {
        children = new ArrayList<>();
        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
        entryCache = new HashMap<>();
        childHash = 31;
        this.lazy = lazy;
    }

    @Override
    public ListenableFuture<V> reload(final K key, final V oldValue) {

        if (log.isTraceEnabled())
            log.trace("Reload the cache");

        ListenableFutureTask<V> task = null;
        if (!lazy) {
            if (log.isTraceEnabled())
                log.trace("Reloading synchronously");
            try {
                build(null);
                return Futures.immediateFuture(load(key));

            } catch (Exception e) {
                log.error(e);
            }
        } else {

            task = ListenableFutureTask.create(() -> {
                build(null);
                return load(key);
            });

            executor.execute(task);
        }

        return task;

    }

    public Loader<K,V> nestChild(Loader<K,V> child) {
        if (!children.contains(child)) {
            children.add(child);
            synchronized (entryCache) {
                entryCache.clear();
            }
        } else {
            if (log.isTraceEnabled())
                log.trace("Cache already contains " + child);
        }
        return this;
    }

    protected abstract void merge(K key, V value) throws Exception;

    protected abstract void build(K key) throws Exception;

    protected void buildChildren(K key) throws Exception {
        for (Loader<K,V> child : children) {
            child.build(key);
            // Map<K,V>
            for (Entry<K,V> entry : child.entryCache.entrySet()) {
                V v = entryCache.get(entry.getKey());
                if (null == v) {
                    entryCache.put(entry.getKey(), entry.getValue());
                } else
                    merge(entry.getKey(), entry.getValue());

            }

        }

    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public V load(K key) throws Exception {

        if (entryCache.isEmpty()) {
            if (log.isTraceEnabled())
                log.trace("Building initial cache");

            buildChildren(key);

            build(key);
        }

        synchronized (entryCache) {
            if (log.isTraceEnabled())
                log.trace("Getting key " + key);
            return entryCache.get(key);
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            if (log.isDebugEnabled())
                log.debug("Loading cache asynchronously");
            build(null);
        } catch (Throwable e) {
            log.error(e);
        }

    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Loader<?,?> loader = (Loader<?,?>) o;
        return childHash == loader.childHash && lazy == loader.lazy && Objects.equals(entryCache, loader.entryCache)
                        && Objects.equals(executor, loader.executor) && Objects.equals(children, loader.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryCache, executor, children, childHash, lazy);
    }
}

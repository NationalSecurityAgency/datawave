package datawave.zookeeper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a local cache backed by a {@link CuratorCache} instance.
 * <ul>
 * <li>A {@link CuratorCacheListener} will be added to the {@link CuratorCache} to update local cache mirrors to reflect an initial build, or node
 * creation/deletions.</li>
 * <li>A connection state listener will be registered with the client used to create the {@link LocalZkCache} that will automatically mark the
 * {@link LocalZkCache} as unhealthy if the connection ever reaches a state of suspended, lost, or read-only. If the backing client reconnects to Zookeeper, a
 * rebuild of local caches will automatically trigger and the {@link LocalZkCache} will be put back into a healthy state.</li>
 * </ul>
 */
public abstract class LocalZkCache {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * A lock that controls access when the local caches are modified or rebuilt entirely.
     */
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Whether this cache is considered healthy.
     */
    protected final AtomicBoolean healthy = new AtomicBoolean(false);

    /**
     * The Zookeeper cache.
     */
    protected final CuratorCache cache;

    /**
     * Create a new {@link LocalZkCache} with the given Zookeeper client. The underlying Zookeeper cache will not be started, and must be started via
     * {@link CuratorCache#start()}.
     *
     * @param client
     *            the Zookeeper client to use when creating the underlying Zookeeper cache
     */
    protected LocalZkCache(CuratorFramework client) {
        this.cache = createCuratorCache(client);
        this.cache.listenable().addListener(getCuratorCacheListener());
        client.getConnectionStateListenable().addListener(getConnectionStateListener());
    }

    /**
     * Create and return a new {@link CuratorCacheListener} that will perform the following actions based on cache events:
     * <ul>
     * <li>Cache initialization: When the cache has completed initialization, {@link #rebuild()} will be called to build the initial mirror caches.</li>
     * <li>Node creation: When a node has been created, it will be passed to {@link #handleCreate(ChildData)}.</li>
     * <li>Node deletion: When a node has been deleted, it will be passed to {@link #handleDelete(ChildData)}.</li>
     * </ul>
     *
     * @return the listener
     */
    protected CuratorCacheListener getCuratorCacheListener() {
        // @formatter:off
        return CuratorCacheListener.builder()
                        // When the curator cache is finished initializing, trigger a rebuild of the local caches.
                        .forInitialized(() -> {
                            log.trace("Cache initialized");
                            try{
                                rebuild();
                            } catch (Exception e) {
                                log.error("Error occurred while rebuilding cache", e);
                            }
                        })
                        .forCreates((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                if(log.isTraceEnabled()){
                                    log.trace("Node created: {}", child.getPath());
                                }
                                handleCreate(child);
                            } catch (Exception e) {
                                log.error("Error occurred while handling creation of node {}", child.getPath(), e);
                            } finally{
                                lock.readLock().unlock();
                            }
                        })
                        .forDeletes((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                if(log.isTraceEnabled()){
                                    log.trace("Node deleted: {}", child.getPath());
                                }
                                handleDelete(child);
                            } catch (Exception e) {
                                log.error("Error occurred while handling creation of node {}", child.getPath(), e);
                            } finally{
                                lock.readLock().unlock();
                            }
                        })
                        .build();
        // @formatter:on
    }

    /**
     * Return a {@link ConnectionStateListener} that will mark this {@link LocalZkCache} as unhealthy when the backing Zookeeper client connection is suspended,
     * lost, or read-only, and will rebuild the local cache mirrors when the client reconnects to Zookeeper.
     *
     * @return the new state listener
     */
    protected ConnectionStateListener getConnectionStateListener() {
        return (client, newState) -> {
            switch (newState) {
                case CONNECTED:
                    if (log.isTraceEnabled()) {
                        log.trace("Connection state: {}, nothing to do", newState);
                    }
                    break;
                case RECONNECTED:
                    if (log.isTraceEnabled()) {
                        log.trace("Connection state: {}, rebuilding cache", newState);
                    }
                    rebuild();
                    break;
                case SUSPENDED:
                case LOST:
                case READ_ONLY:
                    if (log.isTraceEnabled()) {
                        log.trace("Connection state: {}, marking cache unhealthy", newState);
                    }
                    markUnhealthy();
                    break;
                default:
                    log.warn("Unexpected Zookeeper connection state {}, marking cache unhealthy.", newState);
                    markUnhealthy();
            }
        };
    }

    /**
     * Whether this {@link LocalZkCache} is considered in a healthy state.
     *
     * @return true if healthy, or false otherwise
     */
    public boolean isHealthy() {
        return healthy.get();
    }

    /**
     * Mark this {@link LocalZkCache} as in a healthy state.
     */
    protected void markHealthy() {
        lock.writeLock().lock();
        try {
            healthy.set(true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Mark this {@link LocalZkCache} as in an unhealthy state.
     */
    protected void markUnhealthy() {
        lock.writeLock().lock();
        try {
            healthy.set(false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rebuild the local cache mirrors of this {@link LocalZkCache}, and put the cache back into a healthy state.
     */
    protected void rebuild() {
        log.debug("Rebuilding local caches");
        lock.writeLock().lock();
        try {
            markUnhealthy();

            try {
                rebuildLocalCaches();
            } catch (Exception e) {
                log.error("Error occurred while rebuilding local caches", e);
                throw e;
            }

            log.debug("Local caches rebuilt");

            markHealthy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the new Zookeeper cache that will be used by this {@link LocalZkCache}.
     *
     * @param client
     *            the client to use when creating the cache
     * @return the new cache
     */
    protected abstract CuratorCache createCuratorCache(CuratorFramework client);

    /**
     * Update local cache mirrors to reflect the creation of the given node.
     *
     * @param childData
     *            the node
     */
    protected abstract void handleCreate(ChildData childData);

    /**
     * Update local cache mirrors to reflect the deletion of the given node.
     *
     * @param childData
     *            the node
     */
    protected abstract void handleDelete(ChildData childData);

    /**
     * Rebuild all local cache mirrors.
     */
    protected abstract void rebuildLocalCaches();

    /**
     * Close the backing Zookeeper cache and mark the cache as unhealthy.
     */
    public void close() {
        lock.writeLock().lock();
        try {
            markUnhealthy();

            try {
                this.cache.close();
            } catch (Exception e) {
                log.warn("Failed to close curator cache", e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

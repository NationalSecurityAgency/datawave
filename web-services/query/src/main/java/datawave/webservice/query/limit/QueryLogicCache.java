package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERY_LOGICS_ROOT_PATH;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
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
 * This class maintains a local mirror of query logics stored in Zookeeper when tracking active queries.
 */
public class QueryLogicCache {

    private static final Logger log = LoggerFactory.getLogger(QueryLogicCache.class);

    /**
     * The starting index to use when extracting a query logic from a distinct query logic path.
     */
    private static final int QUERY_LOGIC_SUBSTRING_START_INDEX = QUERY_LOGICS_ROOT_PATH.length() + 1;

    /**
     * The Zookeeper cache.
     */
    private final CuratorCache cache;

    /**
     * The local mirror of query logics. Writes are expected to happen infrequently.
     */
    private final Set<String> queryLogics = new CopyOnWriteArraySet<>();

    /**
     * The list of listeners that will be notified of updates to {@link #queryLogics}. Writes are expected to happen infrequently.
     */
    private final List<QueryLogicsUpdateListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * A lock that controls access when either adding/removing query logics to the cache, or rebuilding the cache in its entirety.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Whether this cache is considered healthy.
     */
    private final AtomicBoolean healthy = new AtomicBoolean(false);

    public QueryLogicCache(CuratorFramework client) {
        this.cache = CuratorCache.build(client, QUERY_LOGICS_ROOT_PATH);
        this.cache.listenable().addListener(getCacheListener());
        this.cache.start();
        client.getConnectionStateListenable().addListener(getStateListener());
    }

    /**
     * Build and return a listener that will:
     * <ul>
     * <li>Store any pre-existing query logics in the local mirror when the cache is initialized.</li>
     * <li>Add created query logics to the local mirror and pass it to any configured listeners.</li>
     * <li>Remove deleted query logics from the local mirror and pass it to any configured listeners.</li>
     * </ul>
     *
     * @return the listener
     */
    private CuratorCacheListener getCacheListener() {
        // @formatter:off
        return CuratorCacheListener.builder()
                        // When the cache is first initialized, fetch the set of distinct query logics that already exist in Zookeeper (these nodes are not
                        // ephemeral) and add them to the set of query logics.
                        .forInitialized(() -> {
                            rebuild();
                            if(log.isDebugEnabled()) {
                                log.debug("Initialized with {} query logics ", queryLogics.size());
                            }
                        })
                        // When a query logic node is created, extract the query logic, add it to the local cache, and notify listeners of its creation.
                        .forCreates((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                String path = child.getPath();
                                if(!path.equals(QUERY_LOGICS_ROOT_PATH)) {
                                    String queryLogic = getQueryLogic(path);
                                    if(log.isDebugEnabled()) {
                                        log.debug("Query logic added: {}", queryLogic);
                                    }
                                    this.queryLogics.add(queryLogic);
                                    this.listeners.forEach((l) -> l.forCreate(queryLogic));
                                }
                            } finally{
                                lock.readLock().unlock();
                            }
                        })
                        // When a query logic node is deleted, extract the query logic, remove it from the local cache, and notify listeners of its deletion.
                        // A deletion event should only occur if the query logic nodes are cleaned up manually while the system is running.
                        .forDeletes((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                String path = child.getPath();
                                if(!path.equals(QUERY_LOGICS_ROOT_PATH)) {
                                    String queryLogic = getQueryLogic(path);
                                    this.queryLogics.remove(queryLogic);
                                    if(log.isDebugEnabled()) {
                                        log.debug("Query logic node removed: {}", queryLogic);
                                    }
                                    this.listeners.forEach((l) -> l.forDelete(queryLogic));
                                }
                            } finally{
                                lock.readLock().unlock();
                            }
                        }).build();
        // @formatter:on
    }

    /**
     * Return a {@link ConnectionStateListener} that will mark this {@link QueryLogicCache} as unhealthy when the backing Zookeeper client connection is
     * suspended, lost, or read-only, and will rebuild the cache when the client reconnects to Zookeeper.
     *
     * @return the new state listener
     */
    private ConnectionStateListener getStateListener() {
        return (client, newState) -> {
            switch (newState) {
                case CONNECTED:
                    if (log.isTraceEnabled()) {
                        log.trace("Connection state: {}", newState);
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
                    healthy.set(false);
                    break;
                default:
                    log.warn("Unexpected Zookeeper connection state {}", newState);
                    throw new IllegalStateException("Unxpected Zookeeper connection state: " + newState);
            }
        };
    }

    /**
     * Return whether this {@link QueryLogicCache} is considered in a healthy state.
     *
     * @return whether this {@link QueryLogicCache} is considered healthy
     */
    public boolean isHealthy() {
        return healthy.get();
    }

    /**
     * Get the set of query logics.
     *
     * @return the query logics
     */
    public Set<String> getQueryLogics() {
        return Collections.unmodifiableSet(this.queryLogics);
    }

    /**
     * Add a listener that will be notified of query logic additions and removals.
     *
     * @param listener
     *            the listener
     */
    public void addListener(QueryLogicsUpdateListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Rebuild the local mirror of the query logics.
     */
    public void rebuild() {
        log.debug("Rebuilding cache");
        // Exclusive lock. Blocks all forCreates/forDeletes events until the rebuild is complete.
        lock.writeLock().lock();
        try {
            // Mark the cache as unhealthy and clear the local mirror set.
            this.healthy.set(false);
            this.queryLogics.clear();

            // Update the local mirror set with the query logics.
            // @formatter:off
            this.cache.stream().map(ChildData::getPath)
                            .filter((path) -> !path.equals(QUERY_LOGICS_ROOT_PATH))
                            .map(QueryLogicCache::getQueryLogic)
                            .forEach(this.queryLogics::add);
            // @formatter:on
            log.debug("Cache rebuild complete");

            // Mark the cache as healthy.
            this.healthy.set(true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the query logic portion from a path of the format {@code /distinctQueryLogic/<queryLogic>}.
     *
     * @param path
     *            the path
     * @return the query logic
     */
    private static String getQueryLogic(String path) {
        return path.substring(QUERY_LOGIC_SUBSTRING_START_INDEX);
    }

    /**
     * Closes the underlying Zookeeper cache and clears the listeners and query logic set.
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            this.healthy.set(false);

            try {
                this.cache.close();
            } catch (Exception e) {
                log.warn("Error closing curator cache", e);
            }

            this.listeners.clear();
            this.queryLogics.clear();
        } catch (Exception e) {
            log.warn("Error closing query logic cache", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

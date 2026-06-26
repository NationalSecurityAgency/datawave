package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERIES_ROOT_PATH;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains a local mirror of active query counts for users and systems based off active queries stored in Zookeeper.
 */
public class QueryCountsCache {

    private static final Logger log = LoggerFactory.getLogger(QueryCountsCache.class);

    /**
     * The Zookeeper cache.
     */
    private final CuratorCache cache;

    /**
     * The local cache of total active queries per user that reflects the activity within Zookeeper.
     */
    private final ConcurrentHashMap<String,AtomicInteger> userQueryCounts = new ConcurrentHashMap<>();

    /**
     * The local cache of total active queries per user and query logic that reflects the activity within Zookeeper.
     */
    private final ConcurrentHashMap<String,AtomicInteger> userQueryLogicCounts = new ConcurrentHashMap<>();

    /**
     * The local cache of total active queries per system that reflects the activity within Zookeeper.
     */
    private final ConcurrentHashMap<String,AtomicInteger> systemQueryCounts = new ConcurrentHashMap<>();

    /**
     * The local cache of total active queries per system and query logic that reflects the activity within Zookeeper.
     */
    private final ConcurrentHashMap<String,AtomicInteger> systemQueryLogicCounts = new ConcurrentHashMap<>();

    /**
     * A lock that controls access when either modifying the count maps or rebuilding them in their entirety.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Whether this cache is considered healthy.
     */
    private final AtomicBoolean healthy = new AtomicBoolean(false);

    /**
     * The system limit provider. This is used to determine whether systems count towards user limits when incrementing counts.
     */
    private SystemLimitProvider systemLimitProvider;

    public QueryCountsCache(CuratorFramework client, SystemLimitProvider systemLimitProvider) {
        this.systemLimitProvider = systemLimitProvider;
        this.cache = CuratorCache.build(client, QUERIES_ROOT_PATH);
        this.cache.listenable().addListener(getCacheListener());
        this.cache.start();
        client.getConnectionStateListenable().addListener(getStateListener());
    }

    private CuratorCacheListener getCacheListener() {
        // @formatter:off
        return CuratorCacheListener.builder()
                        .forInitialized(() -> {
                            rebuild();
                            log.info("Cache Initialized");
                        })
                        .forCreates((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                if(!child.getPath().equals(QUERIES_ROOT_PATH)) {
                                    incrementCounts(child.getData());
                                }
                            } finally{
                                lock.readLock().unlock();
                            }
                        })
                        .forDeletes((child) -> {
                            // Use a read lock that allows concurrent create/delete events, but blocks if rebuild() is holding the write lock.
                            lock.readLock().lock();
                            try{
                                if(!child.getPath().equals(QUERIES_ROOT_PATH)) {
                                    decrementCounts(child.getData());
                                }
                            } finally{
                                lock.readLock().unlock();
                            }
                        })
                        .build();
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
                    markUnhealthy();
                    break;
            }
        };
    }

    /**
     * Increment counts based on the user, system, and query logic extracted from the given node data.
     *
     * @param data
     *            the node data
     */
    private void incrementCounts(byte[] data) {
        try {
            // Parse the query information from the node data.
            Triple<String,String,String> query = parseData(data);
            String userDn = query.getLeft();
            String system = query.getMiddle();
            String queryLogic = query.getRight();

            if (log.isTraceEnabled()) {
                log.trace("Incrementing counts for user: {}, system: {}, queryLogic: {}", userDn, system, queryLogic);
            }

            // Increment the counts for the system maps, adding mappings if required.
            systemQueryCounts.computeIfAbsent(system, k -> new AtomicInteger()).getAndIncrement();
            String systemKey = getOwnerQueryLogicKey(system, queryLogic);
            systemQueryLogicCounts.computeIfAbsent(systemKey, k -> new AtomicInteger()).getAndIncrement();

            // If the system counts against user limits, increment the counts for the user maps, adding mappings if required.
            if (systemLimitProvider.countsAgainstUserLimit(system)) {
                userQueryCounts.computeIfAbsent(userDn, k -> new AtomicInteger()).getAndIncrement();
                String userKey = getOwnerQueryLogicKey(userDn, queryLogic);
                userQueryLogicCounts.computeIfAbsent(userKey, k -> new AtomicInteger()).getAndIncrement();
            }
        } catch (Exception e) {
            log.error("Failed to increment counts", e);
            throw new QueryLimiterException("Failed to increment counts", e);
        }
    }

    /**
     * Decrement counts based on the user, system, and query logic extracted from the given node data.
     *
     * @param data
     *            the node data
     */
    private void decrementCounts(byte[] data) {
        try {
            // Parse the query information from the data.
            Triple<String,String,String> query = parseData(data);
            String userDn = query.getLeft();
            String system = query.getMiddle();
            String queryLogic = query.getRight();

            if (log.isTraceEnabled()) {
                log.trace("Decrementing counts for user: {}, system: {}, queryLogic: {}", userDn, system, queryLogic);
            }

            // Decrement the counts for the system maps, removing mappings if the final count is 0.
            systemQueryCounts.computeIfPresent(system, (k, v) -> v.decrementAndGet() == 0 ? null : v);
            String systemKey = getOwnerQueryLogicKey(system, queryLogic);
            systemQueryLogicCounts.computeIfPresent(systemKey, (k, v) -> v.decrementAndGet() == 0 ? null : v);

            // If the system counts against user limits, decrement the counts for the user maps, removing mappings if the final count is 0.
            if (systemLimitProvider.countsAgainstUserLimit(system)) {
                userQueryCounts.computeIfPresent(userDn, (k, v) -> v.decrementAndGet() == 0 ? null : v);
                String userKey = getOwnerQueryLogicKey(userDn, queryLogic);
                userQueryLogicCounts.computeIfPresent(userKey, (k, v) -> v.decrementAndGet() == 0 ? null : v);
            }
        } catch (Exception e) {
            log.error("Failed to decremment counts", e);
            throw new QueryLimiterException("Failed to decrement counts", e);
        }
    }

    /**
     * Return whether this {@link QueryCountsCache} is considered in a healthy state.
     *
     * @return this {@link QueryCountsCache} is considered healthy
     */
    public boolean isHealthy() {
        return healthy.get();
    }

    /**
     * Mark this {@link QueryCountsCache} as in an unhealthy state.
     */
    public void markUnhealthy() {
        healthy.set(false);
    }

    /**
     * Rebuild the internal cache of query counts. The method {@link #isHealthy()} will return false until the rebuild completes.
     */
    public void rebuild() {
        log.debug("Rebuilding cache");
        // Exclusive lock. Blocks all forCreates/forDeletes events until the rebuild is complete.
        lock.writeLock().lock();
        try {
            // Mark the cache as unhealthy.
            healthy.set(false);

            // Clear the count maps.
            userQueryCounts.clear();
            userQueryLogicCounts.clear();
            systemQueryCounts.clear();
            systemQueryLogicCounts.clear();

            // Recount the active queries.
            // @formatter:off
            cache.stream().filter((node) -> !node.getPath().equals(QUERIES_ROOT_PATH))
                            .map(ChildData::getData)
                            .forEach(this::incrementCounts);
            // @formatter:on

            log.debug("Cache rebuild complete");
            healthy.set(true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Set the {@link SystemLimitProvider} for this {@link QueryCountsCache}. This will trigger a rebuild of the internal count maps.
     *
     * @param systemLimitProvider
     *            the system limit provider
     */
    public void setSystemLimitProvider(SystemLimitProvider systemLimitProvider) {
        lock.writeLock().lock();
        try {
            this.systemLimitProvider = systemLimitProvider;
            rebuild();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the total active queries for the given user.
     *
     * @param userDn
     *            the user DN
     * @return the total active queries
     */
    public int getTotalUserQueries(String userDn) {
        return getCount(userDn, userQueryCounts);
    }

    /**
     * Return the total active queries for the given user with the given query logic.
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @return the total active queries
     */
    public int getTotalUserQueries(String userDn, String queryLogic) {
        return getCount(getOwnerQueryLogicKey(userDn, queryLogic), userQueryLogicCounts);
    }

    /**
     * Return the total active queries for the given system.
     *
     * @param system
     *            the system
     * @return the total active queries
     */
    public int getTotalSystemQueries(String system) {
        return getCount(system, systemQueryCounts);
    }

    /**
     * Return the total active queries for the given system with the given query logic.
     *
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @return the total active queries
     */
    public int getTotalSystemQueries(String system, String queryLogic) {
        return getCount(getOwnerQueryLogicKey(system, queryLogic), systemQueryLogicCounts);
    }

    /**
     * Return the count in the given map for the given key after obtaining a read lock for the given lock.
     *
     * @param key
     *            the map key
     * @param map
     *            the map
     * @return the count for the given key in the map, defaulting to 0
     */
    private int getCount(String key, ConcurrentHashMap<String,AtomicInteger> map) {
        AtomicInteger count = map.get(key);
        return count == null ? 0 : count.get();
    }

    /**
     * Return the given owner and query logic as a String joined by a colon.
     *
     * @param owner
     *            the owner
     * @param queryLogic
     *            the query logic
     * @return the combined String
     */
    private static String getOwnerQueryLogicKey(String owner, String queryLogic) {
        return owner + ":" + queryLogic;
    }

    /**
     * Parses and returns a {@link Triple} with the user DN, system, and query logic in that order.
     *
     * @param data
     *            the order to parse
     * @return the triple
     */
    private static Triple<String,String,String> parseData(byte[] data) {
        try {
            DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
            String userDn = WritableUtils.readString(dataInput);
            String system = WritableUtils.readString(dataInput);
            String queryLogic = WritableUtils.readString(dataInput);
            return Triple.of(userDn, system, queryLogic);
        } catch (Exception e) {
            throw new QueryLimiterException("Failed to parse data byte array", e);
        }
    }

    /**
     * Closes the underlying Zookeeper cache and clears the count maps.
     */
    public void close() {
        lock.writeLock().lock();
        try {
            this.healthy.set(false);

            try {
                this.cache.close();
            } catch (Exception e) {
                log.error("Error closing curator cache", e);
            }

            this.userQueryCounts.clear();
            this.userQueryLogicCounts.clear();
            this.systemQueryCounts.clear();
            this.systemQueryLogicCounts.clear();
        } catch (Exception e) {
            log.error("Error closing query count cache", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

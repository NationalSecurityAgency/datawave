package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERIES_ROOT_PATH;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

import datawave.zookeeper.LocalZkCache;

/**
 * This class maintains a local mirror of active query counts for users and systems based off active queries stored in Zookeeper.
 */
public class QueryCountsCache extends LocalZkCache {

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
     * The system limit provider. This is used to determine whether systems count towards user limits when incrementing counts.
     */
    private SystemLimitProvider systemLimitProvider;

    public QueryCountsCache(CuratorFramework client, SystemLimitProvider systemLimitProvider) {
        super(client, false);
        this.systemLimitProvider = systemLimitProvider;
        this.cache.start();
    }

    @Override
    protected CuratorCache createCuratorCache(CuratorFramework client) {
        return CuratorCache.build(client, QUERIES_ROOT_PATH);
    }

    @Override
    protected void handleCreate(ChildData childData) {
        if (isNotCacheRoot(childData)) {
            modifyCount(childData, 1);
        }
    }

    @Override
    protected void handleDelete(ChildData childData) {
        if (isNotCacheRoot(childData)) {
            modifyCount(childData, -1);
        }
    }

    @Override
    protected void rebuildLocalCaches() {
        lock.writeLock().lock();
        try {
            // Clear the count maps.
            userQueryCounts.clear();
            userQueryLogicCounts.clear();
            systemQueryCounts.clear();
            systemQueryLogicCounts.clear();

            // Recount the active queries.
            cache.stream().filter((node) -> !node.getPath().equals(QUERIES_ROOT_PATH)).forEach((child) -> modifyCount(child, 1));
        } finally {
            lock.writeLock().unlock();
        }

    }

    /**
     * Modify the counts for the user, system, and query logic extracted from the given node by the given delta.
     *
     * @param node
     *            the node
     * @param delta
     *            the delta
     */
    private void modifyCount(ChildData node, int delta) {
        String userDn;
        String system;
        String queryLogic;
        try {
            // Parse the query information from the data.
            Triple<String,String,String> query = parseData(node.getData());
            userDn = query.getLeft();
            system = query.getMiddle();
            queryLogic = query.getRight();
        } catch (Exception e) {
            log.error("Failed to parse data for node {}", node.getPath(), e);
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("Incrementing counts for user: {}, system: {}, queryLogic: {} by {}", userDn, system, queryLogic, delta);
        }

        try {
            modifyCount(systemQueryCounts, system, delta);
            modifyCount(systemQueryLogicCounts, getOwnerQueryLogicKey(system, queryLogic), delta);

            if (systemLimitProvider.countsAgainstUserLimit(system)) {
                modifyCount(userQueryCounts, userDn, delta);
                modifyCount(userQueryLogicCounts, getOwnerQueryLogicKey(userDn, queryLogic), delta);
            }
        } catch (Exception e) {
            log.error("Failed to increment counts for user: {}, system: {}, queryLogic: {} by {} for node {}", userDn, system, queryLogic, delta,
                            node.getPath(), e);
        }
    }

    /**
     * Compute the value for the mapping with the given key in the given map after applying the given delta.
     *
     * @param map
     *            the map
     * @param key
     *            the mapping key
     * @param delta
     *            the value delta
     */
    private void modifyCount(Map<String,AtomicInteger> map, String key, int delta) {
        map.compute(key, (k, existingValue) -> {
            // If there is no existing mapping, return a new mapping only if the value will be greater than 0.
            if (existingValue == null) {
                return delta > 0 ? new AtomicInteger(delta) : null;
            } else {
                // If there is an existing mapping, modify the value by the delta. If the updated value is 0 or greater, delete the mapping. Otherwise, return
                // the updated value.
                return existingValue.addAndGet(delta) <= 0 ? null : existingValue;
            }
        });
    }

    /**
     * Set the {@link SystemLimitProvider} for this {@link QueryCountsCache}. This will trigger a rebuild of the internal count maps.
     *
     * @param systemLimitProvider
     *            the system limit provider
     */
    public void setSystemLimitProvider(SystemLimitProvider systemLimitProvider) {
        Preconditions.checkNotNull(systemLimitProvider, "system limit provider cannot be null");
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
        return count != null ? count.get() : 0;
    }

    private static boolean isNotCacheRoot(ChildData childData) {
        return !childData.getPath().equals(QUERIES_ROOT_PATH);
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
     * Closes this {@link QueryCountsCache} and clears the local caches.
     *
     * @see LocalZkCache#close()
     */
    public void close() {
        lock.writeLock().lock();
        try {
            // Close the backing Zookeeper cache.
            super.close();

            // Clear the local caches.
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

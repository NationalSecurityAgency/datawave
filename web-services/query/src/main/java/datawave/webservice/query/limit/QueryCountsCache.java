package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERIES_ROOT_PATH;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Map;
import java.util.Set;
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
     * Tracks which node paths have already had their counts applied, keyed by ZK path. This makes count application idempotent. A create event for a path
     * already present here is a no-op, and a delete event for a path not present here is a no-op.
     */
    private final Set<String> countedQueries = ConcurrentHashMap.newKeySet();

    /**
     * The system limit provider. This is used to determine whether systems count towards user limits when incrementing counts.
     */
    private SystemLimitProvider systemLimitProvider;

    public QueryCountsCache(CuratorFramework client, SystemLimitProvider systemLimitProvider) {
        super(client);
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
            applyCreate(childData);
        }
    }

    @Override
    protected void handleDelete(ChildData childData) {
        if (isNotCacheRoot(childData)) {
            applyDelete(childData);
        }
    }

    @Override
    protected void rebuildLocalCaches() {
        lock.writeLock().lock();
        try {
            // Clear the count maps and the idempotency-tracking set together, so they stay consistent.
            countedQueries.clear();
            userQueryCounts.clear();
            userQueryLogicCounts.clear();
            systemQueryCounts.clear();
            systemQueryLogicCounts.clear();

            // Recount the active queries.
            cache.stream().filter((node) -> !node.getPath().equals(QUERIES_ROOT_PATH)).forEach(this::applyCreate);
        } finally {
            lock.writeLock().unlock();
        }

    }

    private void applyCreate(ChildData node) {
        Triple<String,String,String> data;
        try {
            data = parseData(node.getData());
        } catch (Exception e) {
            log.error("Failed to parse data for node {}", node.getPath(), e);
            return;
        }

        // add() returns true only if the path wasn't already tracked, which is the only case where we want to increment the counts.
        if (countedQueries.add(node.getPath())) {
            if (log.isTraceEnabled()) {
                log.trace("Incrementing counts for user: {}, system: {}, queryLogic: {} for node {}", data.getLeft(), data.getMiddle(), data.getRight(),
                                node.getPath());
            }
            adjustCounts(data, 1);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Node {} was already counted, skipping duplicate create event", node.getPath());
            }
        }
    }

    private void applyDelete(ChildData node) {
        // remove() returns true only if the path was tracked, which is the only case where we want to decrement the counts.
        if (countedQueries.remove(node.getPath())) {
            Triple<String,String,String> data;
            try {
                data = parseData(node.getData());
            } catch (Exception e) {
                log.error("Failed to parse data for node {}", node.getPath(), e);
                return;
            }
            if (log.isTraceEnabled()) {
                log.trace("Decrementing counts for user: {}, system: {}, queryLogic: {} for node {}", data.getLeft(), data.getMiddle(), data.getRight(),
                                node.getPath());
            }
            adjustCounts(data, -1);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Node {} was not tracked as counted, skipping delete event", node.getPath());
            }
        }
    }

    /**
     * Adjust the counts for the user, system, and query logic extracted from the given node by the given delta.
     *
     * @param data
     *            the data parsed from the node
     * @param delta
     *            the delta
     */
    private void adjustCounts(Triple<String,String,String> data, int delta) {
        String userDn = data.getLeft();
        String system = data.getMiddle();
        String queryLogic = data.getRight();
        try {
            modifyCount(systemQueryCounts, system, delta);
            modifyCount(systemQueryLogicCounts, getOwnerQueryLogicKey(system, queryLogic), delta);

            if (systemLimitProvider.countsAgainstUserLimit(system)) {
                modifyCount(userQueryCounts, userDn, delta);
                modifyCount(userQueryLogicCounts, getOwnerQueryLogicKey(userDn, queryLogic), delta);
            }
        } catch (Exception e) {
            log.error("Failed to adjusts counts for user: {}, system: {}, queryLogic: {} by {}", userDn, system, queryLogic, delta, e);
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
                // If there is an existing mapping, modify the value by the delta. If the updated value is 0 or less, delete the mapping. Otherwise, return
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
        lock.readLock().lock();
        try {
            AtomicInteger count = map.get(key);
            return count != null ? count.get() : 0;
        } finally {
            lock.readLock().unlock();
        }
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
     *            the dataorder to parse
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
    @Override
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

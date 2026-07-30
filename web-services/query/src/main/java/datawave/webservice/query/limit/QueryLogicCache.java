package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERY_LOGICS_ROOT_PATH;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.zookeeper.LocalZkCache;

/**
 * This class maintains a local mirror of query logics stored in Zookeeper when tracking active queries.
 */
public class QueryLogicCache extends LocalZkCache {

    private static final Logger log = LoggerFactory.getLogger(QueryLogicCache.class);

    /**
     * The starting index to use when extracting a query logic from a distinct query logic path.
     */
    private static final int QUERY_LOGIC_SUBSTRING_START_INDEX = QUERY_LOGICS_ROOT_PATH.length() + 1;

    /**
     * The local mirror of query logics. Writes are expected to happen infrequently.
     */
    private final Set<String> queryLogics = new CopyOnWriteArraySet<>();

    /**
     * The list of listeners that will be notified of updates to {@link #queryLogics}. Writes are expected to happen infrequently.
     */
    private final List<QueryLogicsUpdateListener> listeners = new CopyOnWriteArrayList<>();

    public QueryLogicCache(CuratorFramework client) {
        super(client, true);
    }

    @Override
    protected CuratorCache createCuratorCache(CuratorFramework client) {
        return CuratorCache.build(client, QUERY_LOGICS_ROOT_PATH);
    }

    @Override
    protected void handleCreate(ChildData childData) {
        if (isNotCacheRoot(childData)) {
            String queryLogic = getQueryLogic(childData.getPath());
            if (log.isDebugEnabled()) {
                log.debug("Adding query logic: {}", queryLogic);
            }
            this.queryLogics.add(queryLogic);
            this.listeners.forEach((listener) -> {
                try {
                    listener.forCreate(queryLogic);
                } catch (Exception e) {
                    log.error("Error throw by listener {} notified of creation of query logic {}", listener, queryLogic, e);
                }
            });
        }
    }

    @Override
    protected void handleDelete(ChildData childData) {
        if (isNotCacheRoot(childData)) {
            String queryLogic = getQueryLogic(childData.getPath());
            if (log.isTraceEnabled()) {
                log.trace("Deleting query logic: {}", queryLogic);
            }
            this.queryLogics.remove(queryLogic);
            this.listeners.forEach((listener) -> {
                try {
                    listener.forDelete(queryLogic);
                } catch (Exception e) {
                    log.error("Error thrown by listener {} notified of deletion of query logic {}", listener, queryLogic, e);
                }
            });
        }
    }

    @Override
    protected void rebuildLocalCaches() {
        lock.writeLock().lock();
        try {
            this.queryLogics.clear();
            // Update the local mirror set with the query logics.
            // @formatter:off
            this.cache.stream().filter(QueryLogicCache::isNotCacheRoot)
                            .map(ChildData::getPath)
                            .forEach((path) -> {
                                try{
                                    String queryLogic = getQueryLogic(path);
                                    this.queryLogics.add(queryLogic);
                                } catch (Exception e){
                                    log.error("Error occurred while adding query logic from node {}", path, e);
                                }
                            });
            if(log.isTraceEnabled()) {
                log.debug("Rebuilt with querylogics: {}", this.queryLogics);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the set of query logics.
     *
     * @return the query logics
     */
    public Set<String> getQueryLogics() {
        return Set.copyOf(this.queryLogics);
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
     * Return whether the given node does not have the path {@value QueryLimiterUtils#QUERY_LOGICS_ROOT_PATH}.
     * @param childData the node
     * @return true if the node does not have
     */
    private static boolean isNotCacheRoot(ChildData childData) {
        return !childData.getPath().equals(QUERY_LOGICS_ROOT_PATH);
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
     * Closes this {@link QueryLogicCache} and clears the local cache.
     * @see LocalZkCache#close()
     */
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            // Close the backing Zookeeper cache.
            super.close();


            this.listeners.clear();
            this.queryLogics.clear();
        } catch (Exception e) {
            log.warn("Error closing query logic cache", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

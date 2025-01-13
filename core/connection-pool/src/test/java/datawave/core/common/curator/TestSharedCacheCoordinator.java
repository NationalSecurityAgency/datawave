package datawave.core.common.curator;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PreDestroy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.common.cache.SharedBoolean;
import datawave.core.common.cache.SharedBooleanListener;
import datawave.core.common.cache.SharedCacheCoordinator.ArgumentChecker;

/**
 * Coordinates operations on a shared cache. That is, this coordinates operations where an in-memory cache may be running on multiple servers and each in-memory
 * cache is using a shared backing store (e.g., shared filesystem, Accumulo, etc). There are helper methods to handle distributed locking, notification of
 * global cache operations (flush, evict), and eviction notices.
 * <p>
 * This coordinator uses Zookeeper.
 */

public class TestSharedCacheCoordinator implements Serializable {
    public interface EvictionCallback {
        void evict(String dn);
    }

    private static final long serialVersionUID = 1L;
    private static final String LIVE_SERVERS = "/liveServers";
    private static final long EVICT_MESSAGE_TIMEOUT = 60 * 1000L;

    private Logger log = LoggerFactory.getLogger(getClass());
    private transient CuratorFramework curatorClient;
    private String localName;
    private String serverIdentifierPath;

    private int evictionReaperIntervalInSeconds;
    private int numLocks;

    private final HashMap<Integer,InterProcessLock> locks;
    private final HashMap<String,Integer> localCounters;
    private transient HashMap<String,SharedCount> sharedCounters;

    private final HashMap<String,Boolean> localBooleans;
    private transient HashMap<String,SharedBoolean> sharedBooleans;

    private transient PathChildrenCache evictionPathCache;
    private transient Timer evictionReaper;

    /**
     * Constructs a new {@link TestSharedCacheCoordinator}
     *
     * @param namespace
     *            the Zookeeper namespace to use for grouping all entries created by this coordinator
     * @param zookeeperConnectionString
     *            the Zookeeper connection to use
     */
    public TestSharedCacheCoordinator(String namespace, String zookeeperConnectionString, int evictionReaperIntervalInSeconds, int numLocks) {
        ArgumentChecker.notNull(namespace, zookeeperConnectionString);

        locks = new HashMap<>();
        localCounters = new HashMap<>();
        localBooleans = new HashMap<>();
        sharedCounters = new HashMap<>();
        sharedBooleans = new HashMap<>();
        this.numLocks = numLocks;
        this.evictionReaperIntervalInSeconds = evictionReaperIntervalInSeconds;

        curatorClient = CuratorFrameworkFactory.builder().namespace(namespace).retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .connectString(zookeeperConnectionString).build();

        evictionReaper = new Timer("cache-eviction-reaper", true);
    }

    public void start() {
        curatorClient.start();

        String rootPath = ZKPaths.makePath("/", "evictions");
        try {
            curatorClient.newNamespaceAwareEnsurePath(rootPath).ensure(curatorClient.getZookeeperClient());
        } catch (Exception e) {
            // Wrap the checked exception as a runtime one since the CDI spec says we can't have checked
            // exceptions on a PostConstruct method.
            throw new RuntimeException(e.getMessage(), e);
        }
        evictionPathCache = new PathChildrenCache(curatorClient, "/evictions", true);

        try {
            evictionPathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            throw new RuntimeException("Unable to start eviction path cache: " + e.getMessage(), e);
        }

        // Register ourselves with zookeeper so we know which web servers are available. We use an ephemeral node
        // so that zookeeper will clean up for us when the server goes away.
        // We'll use this list to coordinate DN-based eviction.
        try {
            localName = System.currentTimeMillis() + ManagementFactory.getRuntimeMXBean().getName();
            String serversPath = LIVE_SERVERS;
            curatorClient.newNamespaceAwareEnsurePath(serversPath).ensure(curatorClient.getZookeeperClient());
            serverIdentifierPath = curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                            .forPath(ZKPaths.makePath(serversPath, localName));
        } catch (Exception e) {
            throw new RuntimeException("Couldn't register with zookeeper: " + e.getMessage(), e);
        }

        final long delayPeriod = evictionReaperIntervalInSeconds * 1000L;
        TimerTask reaperTask = new TimerTask() {
            @Override
            public void run() {
                reapEvictions();
            }
        };
        evictionReaper.schedule(reaperTask, delayPeriod, delayPeriod);
    }

    @PreDestroy
    public void stop() {
        evictionReaper.cancel();

        try {
            curatorClient.delete().guaranteed().forPath(serverIdentifierPath);
        } catch (Exception e) {
            log.warn("Error removing server identifier path {}: {}", serverIdentifierPath, e.getMessage(), e);
        }
        for (Entry<String,SharedCount> entry : sharedCounters.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warn("Error closing shared counter {}: {}", entry.getKey(), e.getMessage(), e);
            }
        }
        if (evictionPathCache != null) {
            try {
                evictionPathCache.close();
            } catch (IOException e) {
                log.warn("Error closing eviction monitor: {}", e.getMessage(), e);
            }
        }
        curatorClient.close();
    }

    /**
     * Creates a distributed mutex named {@code path}. This mutex can be used to perform locking across many servers.
     */
    public InterProcessLock getMutex(String path) {
        ArgumentChecker.notNull(path);

        InterProcessLock lock;
        int hash = path.hashCode();
        if (hash < 0)
            hash = -hash;
        int lockNum = hash % numLocks;
        synchronized (locks) {
            lock = locks.get(lockNum);
            if (lock == null) {
                lock = new InterProcessMutex(curatorClient, ZKPaths.makePath("/locks", "lock-" + Integer.toString(lockNum)));
                locks.put(lockNum, lock);
            }
        }

        return lock;
    }

    /**
     * Registers a distributed shared counter named {@code counterName}. This counter can be watched on many servers, and can be used to coordinate local
     * in-memory global operations.
     *
     * @param counterName
     *            the name of the counter
     * @param listener
     *            a listener that is called when the counter value changes
     */
    public void registerCounter(String counterName, SharedCountListener listener) throws Exception {
        ArgumentChecker.notNull(counterName, listener);

        SharedCount count = new SharedCount(curatorClient, ZKPaths.makePath("/counters", counterName), 1);
        count.start();
        sharedCounters.put(counterName, count);
        localCounters.put(counterName, count.getCount());

        count.addListener(listener);
    }

    /**
     * Given the shared counter {@code counterName}, checks wether or not the locally cached value matches the expected shared value of {@code expectedValue}.
     * If the value does not match, the local cached value is updated.
     *
     * @param counterName
     *            the name of the counter whose locally cached value is to be tested
     * @param expectedValue
     *            the shared value/expected local value of the counter
     * @return {@code true} if the local cached value already matches {@code expectedValue} and {@code false} if not (while at the same time updating the local
     *         cached value to {@code expectedValue}
     */
    public boolean checkCounter(String counterName, int expectedValue) {
        ArgumentChecker.notNull(counterName);

        Integer localCount = localCounters.get(counterName);
        Preconditions.checkArgument(localCount != null, "Invalid counter name: " + counterName);

        localCounters.put(counterName, expectedValue);
        return expectedValue == localCount;
    }

    /**
     * Increments the shared distributed counter named {@code counterName} by one.
     */
    public void incrementCounter(String counterName) throws Exception {
        ArgumentChecker.notNull(counterName);

        SharedCount count = sharedCounters.get(counterName);
        Preconditions.checkArgument(count != null, "Invalid counter name: " + counterName);

        int newCount = count.getCount() + 1;
        localCounters.put(counterName, newCount);
        while (!count.trySetCount(newCount)) {
            newCount = count.getCount() + 1;
        }
    }

    /**
     * Increments the shared distributed counter named {@code counterName} by one.
     */
    public void decrementCounter(String counterName) throws Exception {
        ArgumentChecker.notNull(counterName);

        SharedCount count = sharedCounters.get(counterName);
        Preconditions.checkArgument(count != null, "Invalid counter name: " + counterName);

        int newCount = count.getCount() - 1;
        localCounters.put(counterName, newCount);
        while (!count.trySetCount(newCount)) {
            newCount = count.getCount() - 1;
        }
    }

    // //////////////////////

    /**
     * Registers a distributed shared counter named {@code counterName}. This counter can be watched on many servers, and can be used to coordinate local
     * in-memory global operations.
     *
     * @param booleanName
     *            the name of the counter
     * @param listener
     *            a listener that is called when the counter value changes
     */
    public void registerBoolean(String booleanName, SharedBooleanListener listener) throws Exception {
        ArgumentChecker.notNull(booleanName, listener);

        SharedBoolean sharedBoolean = new SharedBoolean(curatorClient, ZKPaths.makePath("/booleans", booleanName), false);
        sharedBoolean.start();
        sharedBooleans.put(booleanName, sharedBoolean);
        localBooleans.put(booleanName, sharedBoolean.getBoolean());
        log.debug("registered a boolean that is {}", sharedBoolean.getBoolean());
        sharedBoolean.addListener(listener);
    }

    /**
     * Given the shared counter {@code counterName}, checks whether or not the locally cached value matches the expected shared value of {@code expectedValue}.
     * If the value does not match, the local cached value is updated.
     *
     * @param booleanName
     *            the name of the counter whose locally cached value is to be tested
     * @param expectedValue
     *            the shared value/expected local value of the counter
     * @return {@code true} if the local cached value already matches {@code expectedValue} and {@code false} if not (while at the same time updating the local
     *         cached value to {@code expectedValue}
     */
    public boolean checkBoolean(String booleanName, boolean expectedValue) {
        ArgumentChecker.notNull(booleanName);

        Boolean localBoolean = localBooleans.get(booleanName);
        Preconditions.checkArgument(localBoolean != null, "Invalid counter name: " + booleanName);

        return expectedValue == localBoolean;
    }

    /**
     * Increments the shared distributed counter named {@code counterName} by one.
     */
    public void setBoolean(String booleanName, boolean state) throws Exception {
        System.err.println("someone wants to setBoolean to " + state);
        ArgumentChecker.notNull(booleanName);

        SharedBoolean sharedBoolean = sharedBooleans.get(booleanName);
        Preconditions.checkArgument(sharedBoolean != null, "Invalid boolean name: " + booleanName);

        boolean newBoolean = state;
        localBooleans.put(booleanName, state);
        while (!sharedBoolean.trySetBoolean(newBoolean)) {
            newBoolean = state;
        }
    }

    /**
     * Sends an eviction message for {@code messagePath} to all other shared cache coordinators that are listening.
     */
    public void sendEvictMessage(String messagePath) throws Exception {
        ArgumentChecker.notNull(messagePath);

        String rootPath = ZKPaths.makePath("/", "evictions");
        String evictMessagePath = ZKPaths.makePath(rootPath, ZKPaths.makePath(messagePath, localName));
        Stat nodeData = curatorClient.checkExists().forPath(evictMessagePath);
        boolean shouldCreate = true;
        if (nodeData != null) {
            long delta = System.currentTimeMillis() - nodeData.getCtime();
            if (delta > EVICT_MESSAGE_TIMEOUT) {
                log.debug("Attempting to delete {} since it was created {}ms ago and hasn't been cleaned up.", evictMessagePath, delta);
                ZKUtil.deleteRecursive(curatorClient.getZookeeperClient().getZooKeeper(), evictMessagePath);
            } else {
                shouldCreate = false;
            }
        }

        if (shouldCreate)
            curatorClient.create().creatingParentsIfNeeded().forPath(evictMessagePath);
    }

    /**
     * Watches the eviction path for new eviction messages. If a message is received, then {@code callback} is invoked, and then zookeeper is updated to
     * indicate this server has responded to the eviction request. Once all running servers have responded, then a cleanup thread running on each server will
     * attempt to remove the eviction request marker so as not to overpopulate zookeeper. All servers may try, but only one will actually delete all of the
     * nodes.
     */
    public void watchForEvictions(final EvictionCallback callback) {
        ArgumentChecker.notNull(callback);

        evictionPathCache.getListenable().addListener((client, event) -> {
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                // Call our eviction handler to do local eviction
                String path = event.getData().getPath();
                String dn = ZKPaths.getNodeFromPath(path);
                callback.evict(dn);

                // Now register ourselves under the eviction node that that once
                // a child for each running web server appears, the eviction node
                // can be cleaned up.
                String responsePath = ZKPaths.makePath(path, localName);
                curatorClient.newNamespaceAwareEnsurePath(responsePath).ensure(curatorClient.getZookeeperClient());
            }
        });
    }

    /**
     * Internal method to check for eviction messages that need to be cleaned up. Servers that have responded to an eviction request place a child node with
     * their name under the eviction message node. When the set of responding servers is the same as or a superset of the list of live servers, then the
     * eviction message node (and its children--the responding server nodes) are removed. We check for a superset, because it is possible that a server
     * responded to the eviction request, but then went away before we could perform this check. If non-existent servers have responded to the request, that's
     * ok. What we don't want are servers that are currently active to have NOT responded to the request before we delete the request node.
     */
    protected void reapEvictions() {
        log.debug("Reaping evict messages");
        try {
            ArrayList<String> pathsToDelete = new ArrayList<>();
            List<String> liveServers = curatorClient.getChildren().forPath(LIVE_SERVERS);
            for (ChildData data : evictionPathCache.getCurrentData()) {
                List<String> responders = curatorClient.getChildren().forPath(data.getPath());
                if (responders.containsAll(liveServers)) {
                    log.debug("{} can be cleaned up.", data.getPath());
                    pathsToDelete.add(data.getPath());
                } else {
                    log.debug("{} cannot be cleaned up: liveServers={}, respondingServers={}", data.getPath(), liveServers, responders);
                }
            }

            for (String path : pathsToDelete) {
                // Try up to 5 times to delete the path and then give up -- we'll try again later, or someone else will.
                for (int i = 0; i < 5 && curatorClient.checkExists().forPath(path) != null; i++) {
                    try {
                        String recursiveDeletePath = ZKPaths.makePath(curatorClient.getNamespace(), path);
                        ZKUtil.deleteRecursive(curatorClient.getZookeeperClient().getZooKeeper(), recursiveDeletePath);
                    } catch (Exception e) {
                        log.trace("Problem deleting {} (this may be ok): {}", path, e.getMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error cleaning up eviction notices: {}", e.getMessage(), e);
        }
    }
}

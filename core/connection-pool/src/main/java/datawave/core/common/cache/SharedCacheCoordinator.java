package datawave.core.common.cache;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Coordinates operations on a shared cache. That is, this coordinates operations where an in-memory cache may be running on multiple servers and each in-memory
 * cache is using a shared backing store (e.g., shared filesystem, Accumulo, etc). There are helper methods to handle distributed locking, notification of
 * global cache operations (flush, evict), and eviction notices.
 * <p>
 * This coordinator uses Zookeeper.
 */
public class SharedCacheCoordinator implements Serializable {
    public interface EvictionCallback {
        void evict(String dn);
    }

    private static final long serialVersionUID = 1L;
    private static final String LIVE_SERVERS = "/liveServers";
    private static final long EVICT_MESSAGE_TIMEOUT = 60 * 1000L;

    private static final Logger log = LoggerFactory.getLogger(SharedCacheCoordinator.class);
    private transient CuratorFramework curatorClient;
    private String localName;
    private String serverIdentifierPath;
    private transient PersistentNode serverIdentifierNode;

    private int evictionReaperIntervalInSeconds;
    private int numLocks;
    private final int maxRetries;

    private final HashMap<Integer,InterProcessLock> locks;
    private final HashMap<String,Integer> localCounters;
    private transient HashMap<String,SharedCount> sharedCounters;
    private transient HashMap<String,SharedCountListener> sharedCountListeners;

    private final HashMap<String,Boolean> localBooleans;
    private transient HashMap<String,SharedBoolean> sharedBooleans;
    private transient HashMap<String,SharedBooleanListener> sharedBooleanListeners;

    private final Map<String,SharedTriState.STATE> localTriStates;
    private transient Map<String,SharedTriState> sharedTriStates;
    private transient Map<String,SharedTriStateListener> sharedTriStateListeners;

    private transient PathChildrenCache evictionPathCache;
    private transient Timer evictionReaper;

    /**
     * Constructs a new {@link SharedCacheCoordinator}
     *
     * @param namespace
     *            the Zookeeper namespace to use for grouping all entries created by this coordinator
     * @param zookeeperConnectionString
     *            the Zookeeper connection to use
     * @param evictionReaperIntervalInSeconds
     *            the eviction interval
     * @param maxRetries
     *            maximum number of retries
     * @param numLocks
     *            number of locks
     */
    public SharedCacheCoordinator(String namespace, String zookeeperConnectionString, int evictionReaperIntervalInSeconds, int numLocks, int maxRetries) {
        ArgumentChecker.notNull(namespace, zookeeperConnectionString);

        locks = new HashMap<>();
        localCounters = new HashMap<>();
        localBooleans = new HashMap<>();

        localTriStates = new HashMap<>();

        sharedCounters = new HashMap<>();
        sharedCountListeners = new HashMap<>();
        sharedBooleans = new HashMap<>();
        sharedBooleanListeners = new HashMap<>();

        sharedTriStates = new HashMap<>();
        sharedTriStateListeners = new HashMap<>();

        this.numLocks = numLocks;
        this.evictionReaperIntervalInSeconds = evictionReaperIntervalInSeconds;
        this.maxRetries = maxRetries;

        curatorClient = CuratorFrameworkFactory.builder().namespace(namespace).retryPolicy(new BoundedExponentialBackoffRetry(100, 5000, 10))
                        .connectString(zookeeperConnectionString).build();

        evictionReaper = new Timer("cache-eviction-reaper-" + namespace, true);
    }

    public void start() {
        curatorClient.start();

        curatorClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            private ConnectionState lastState;

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                switch (newState) {
                    case LOST:
                        shutdownCounters();
                        shutdownBooleans();
                        break;
                    case RECONNECTED:
                        // Re-connect counters after
                        if (lastState == ConnectionState.LOST) {
                            restartCounters();
                            restartBooleans();
                        }
                        break;
                }
                lastState = newState;
            }
        });

        String rootPath = ZKPaths.makePath("/", "evictions");
        try {
            if (curatorClient.checkExists().creatingParentContainersIfNeeded().forPath(rootPath) == null) {
                curatorClient.create().creatingParentContainersIfNeeded().forPath(rootPath);
            }
        } catch (KeeperException.NodeExistsException e) {
            // ignored on purpose -- someone beat us to creating the node
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
            try {
                if (curatorClient.checkExists().creatingParentContainersIfNeeded().forPath(serversPath) == null) {
                    curatorClient.create().creatingParentContainersIfNeeded().forPath(serversPath);
                }
            } catch (KeeperException.NodeExistsException e) {
                // ignored on purpose -- someone beat us to creating the node
            }
            serverIdentifierPath = ZKPaths.makePath(serversPath, localName);
            serverIdentifierNode = new PersistentNode(curatorClient, CreateMode.EPHEMERAL, false, ZKPaths.makePath(serversPath, localName), new byte[0]);
            serverIdentifierNode.start();
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

    private void shutdownCounters() {
        for (String counterName : localCounters.keySet()) {
            SharedCount count = sharedCounters.remove(counterName);
            localCounters.put(counterName, count.getCount());
            try {
                count.removeListener(sharedCountListeners.get(counterName));
                count.close();
            } catch (IOException e) {
                // ignore -- we're going to abandon this counter.
                log.warn("Error closing counter {} after connection lost.", counterName, e);
            }
        }
    }

    private void restartCounters() {
        for (Entry<String,Integer> entry : localCounters.entrySet()) {
            String counterName = entry.getKey();
            try {
                System.out.println("**** RE-REGISTER " + counterName);
                reregisterCounter(counterName, sharedCountListeners.get(counterName), entry.getValue());
            } catch (Exception e) {
                log.error("Unable to re-register shared counter {}", counterName, e);
            }
        }
    }

    private void shutdownBooleans() {
        for (String booleanName : localBooleans.keySet()) {
            SharedBoolean sharedBoolean = sharedBooleans.remove(booleanName);
            localBooleans.put(booleanName, sharedBoolean.getBoolean());
            try {
                sharedBoolean.removeListener(sharedBooleanListeners.get(booleanName));
                sharedBoolean.close();
            } catch (IOException e) {
                // ignore -- we're going to abandon this counter.
                log.warn("Error closing shared boolean {} after connection lost.", booleanName, e);
            }
        }
    }

    private void restartBooleans() {
        for (Entry<String,Boolean> entry : localBooleans.entrySet()) {
            String booleanName = entry.getKey();
            try {
                System.out.println("**** RE-REGISTER " + booleanName);
                reregisterBoolean(booleanName, sharedBooleanListeners.get(booleanName), entry.getValue());
            } catch (Exception e) {
                log.error("Unable to re-register shared boolean {}", booleanName, e);
            }
        }
    }

    private void shutdownTriStates() {
        for (String triStateName : localTriStates.keySet()) {
            SharedTriState sharedTriState = sharedTriStates.remove(triStateName);
            localTriStates.put(triStateName, sharedTriState.getState());
            try {
                sharedTriState.removeListener(sharedTriStateListeners.get(triStateName));
                sharedTriState.close();
            } catch (IOException e) {
                // ignore -- we're going to abandon this counter.
                log.warn("Error closing shared TriState {} after connection lost.", triStateName, e);
            }
        }
    }

    private void restartTriStates() {
        for (Entry<String,SharedTriState.STATE> entry : localTriStates.entrySet()) {
            String triStateName = entry.getKey();
            try {
                System.out.println("**** RE-REGISTER " + triStateName);
                reregisterTriState(triStateName, sharedTriStateListeners.get(triStateName), entry.getValue());
            } catch (Exception e) {
                log.error("Unable to re-register shared TriState {}", triStateName, e);
            }
        }
    }

    public void stop() {
        evictionReaper.cancel();

        try {
            serverIdentifierNode.close();
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
     *
     * @param path
     *            a mutex
     * @return a lock
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
     * @throws Exception
     *             if there are issues
     */
    public void registerCounter(String counterName, SharedCountListener listener) throws Exception {
        Preconditions.checkArgument(!sharedCounters.containsKey(counterName), "Counter " + counterName + " has already been registered!");
        reregisterCounter(counterName, listener, 1);
    }

    private void reregisterCounter(String counterName, SharedCountListener listener, int seedValue) throws Exception {
        ArgumentChecker.notNull(counterName, listener);

        SharedCount count = new SharedCount(curatorClient, ZKPaths.makePath("/counters", counterName), seedValue);
        count.start();
        sharedCounters.put(counterName, count);
        sharedCountListeners.put(counterName, listener);
        localCounters.put(counterName, count.getCount());

        count.addListener(new SharedCountListener() {
            @Override
            public void countHasChanged(SharedCountReader sharedCountReader, int i) throws Exception {}

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED) {
                    try {
                        reregisterCounter(counterName, sharedCountListeners.get(counterName), localCounters.get(counterName));
                    } catch (Exception e) {
                        log.error("Unable to re-register counter {}: {}", counterName, e.getMessage());
                    }
                }
            }
        });
        count.addListener(listener);
    }

    /**
     * Given the shared counter {@code counterName}, checks whether the locally cached value matches the expected shared value of {@code expectedValue}. If the
     * value does not match, the local cached value is updated.
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
     *
     * @param counterName
     *            the counter name
     * @throws Exception
     *             if there are issues
     */
    public void incrementCounter(String counterName) throws Exception {
        ArgumentChecker.notNull(counterName);

        SharedCount count = sharedCounters.get(counterName);
        Preconditions.checkArgument(count != null, "Invalid counter name: " + counterName + ". Shared counter may be down.");

        VersionedValue<Integer> currentCount = count.getVersionedValue();
        int newCount = currentCount.getValue() + 1;
        int tries = 0;
        while (!count.trySetCount(currentCount, newCount)) {
            currentCount = count.getVersionedValue();
            newCount = currentCount.getValue() + 1;
            if (++tries >= maxRetries) {
                // We've exceeded our max tries to update the counter. Try to re-register it and also throw an exception to
                // indicate that we didn't necessarily update the shared count.
                sharedCounters.remove(counterName);
                count.removeListener(sharedCountListeners.get(counterName));
                count.close();
                reregisterCounter(counterName, sharedCountListeners.get(counterName), newCount);
                throw new IllegalStateException(
                                "Unable to increment shared counter " + counterName + " after " + maxRetries + " attempts. Zookeeper connection may be down.");
            }
        }
        localCounters.put(counterName, newCount);
    }

    /**
     * Registers a distributed shared boolean named {@code booleanName}. This boolean can be watched on many servers, and can be used to coordinate local
     * in-memory global operations.
     *
     * @param booleanName
     *            the name of the boolean
     * @param listener
     *            a listener that is called when the boolean value changes
     * @throws Exception
     *             if there are issues
     */
    public void registerBoolean(String booleanName, SharedBooleanListener listener) throws Exception {
        Preconditions.checkArgument(!sharedBooleans.containsKey(booleanName), "Boolean " + booleanName + " has already been registered!");
        reregisterBoolean(booleanName, listener, false);
    }

    private void reregisterBoolean(String booleanName, SharedBooleanListener listener, boolean seedValue) throws Exception {
        ArgumentChecker.notNull(booleanName, listener);

        SharedBoolean sharedBoolean = new SharedBoolean(curatorClient, ZKPaths.makePath("/booleans", booleanName), seedValue);
        if (log.isTraceEnabled()) {
            log.trace("table:{} created a sharedBoolean:{}", booleanName, sharedBoolean);
        }
        sharedBoolean.start();
        sharedBooleans.put(booleanName, sharedBoolean);
        if (log.isTraceEnabled()) {
            log.trace("table:{} sharedBooleans has:{}", booleanName, sharedBooleans);
        }
        localBooleans.put(booleanName, sharedBoolean.getBoolean());
        if (log.isTraceEnabled()) {
            log.trace("table:{} localBooleans has:{}", booleanName, localBooleans);
            log.trace("table:{} registered a boolean that is {}", booleanName, sharedBoolean.getBoolean());
        }

        sharedBoolean.addListener(new SharedBooleanListener() {
            @Override
            public void booleanHasChanged(SharedBooleanReader var1, boolean var2) throws Exception {}

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED) {
                    try {
                        reregisterBoolean(booleanName, sharedBooleanListeners.get(booleanName), localBooleans.get(booleanName));
                    } catch (Exception e) {
                        log.error("Unable to re-register boolean {}: {}", booleanName, e.getMessage());
                    }
                }
            }
        });
        sharedBoolean.addListener(listener);
    }

    /**
     * Given the shared boolean {@code booleanName}, checks whether the locally cached value matches the expected shared value of {@code expectedValue}. If the
     * value does not match, the local cached value is updated.
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

        SharedBoolean sharedBoolean = sharedBooleans.get(booleanName);
        Preconditions.checkArgument(sharedBoolean != null, "Invalid boolean name: " + booleanName);
        if (log.isTraceEnabled()) {
            log.trace("table:{} got {} from {}", booleanName, sharedBoolean, sharedBooleans);
            log.trace("table:{} checking to see if sharedBoolean {} is the same as expected value:{}", booleanName, sharedBoolean, expectedValue);
        }
        return sharedBoolean.getBoolean() == expectedValue;
    }

    /**
     * Sets the shared distributed boolean named {@code booleanName} to the passed state value.
     *
     * @param booleanName
     *            boolean name
     * @param state
     *            the state
     * @throws Exception
     *             if there are issues
     */
    public void setBoolean(String booleanName, boolean state) throws Exception {
        log.trace("table:{} setBoolean( {} )", booleanName, state);
        ArgumentChecker.notNull(booleanName);

        SharedBoolean sharedBoolean = sharedBooleans.get(booleanName);
        Preconditions.checkArgument(sharedBoolean != null, "Invalid boolean name: " + booleanName);
        if (log.isTraceEnabled()) {
            log.trace("table:{} got {} from {}", booleanName, sharedBoolean, sharedBooleans);
        }

        boolean newBoolean = state;
        if (log.isTraceEnabled()) {
            log.trace("table:{} put ( {}, {} ) into localBooleans:{}", booleanName, booleanName, state, localBooleans);
        }
        int tries = 0;
        while (!sharedBoolean.trySetBoolean(newBoolean)) {
            newBoolean = state;
            if (++tries >= maxRetries) {
                // We've exceeded our max tries to update the boolean. Try to re-register it and also throw an exception to
                // indicate that we didn't necessarily update the shared boolean.
                sharedBooleans.remove(booleanName);
                sharedBoolean.removeListener(sharedBooleanListeners.get(booleanName));
                sharedBoolean.close();
                reregisterBoolean(booleanName, sharedBooleanListeners.get(booleanName), newBoolean);
                throw new IllegalStateException("table:" + booleanName + " Unable to update shared boolean " + booleanName + " to " + state + " after "
                                + maxRetries + " attempts. Zookeeper connection may be down.");
            }
        }
        localBooleans.put(booleanName, state);
        if (log.isTraceEnabled()) {
            log.trace("table:{} sharedBoolean now:{}", booleanName, sharedBoolean);
            log.trace("table:{} localBooleans:{}", booleanName, localBooleans);
            log.trace("table:{} sharedBooleans:{}", booleanName, sharedBooleans);
        }
    }

    public void registerTriState(String triStateName, SharedTriStateListener listener) throws Exception {
        Preconditions.checkArgument(!sharedTriStates.containsKey(triStateName), "STATE " + triStateName + " has already been registered!");
        reregisterTriState(triStateName, listener, SharedTriState.STATE.UPDATED);
    }

    private void reregisterTriState(String triStateName, SharedTriStateListener listener, SharedTriState.STATE seedValue) throws Exception {
        ArgumentChecker.notNull(triStateName, listener);

        SharedTriState sharedTriState = new SharedTriState(curatorClient, ZKPaths.makePath("/triStates", triStateName), seedValue);
        if (log.isTraceEnabled())
            log.trace("table:{} created a sharedTriState:{}", triStateName, sharedTriState);
        sharedTriState.start();
        sharedTriStates.put(triStateName, sharedTriState);
        if (log.isTraceEnabled())
            log.trace("table:{} sharedTriStates has:{}", triStateName, sharedTriStates);
        localTriStates.put(triStateName, sharedTriState.getState());
        if (log.isTraceEnabled()) {
            log.trace("table:{} localTriStates has:{}", triStateName, localTriStates);
            log.trace("table:{} registered a TriState that is {}", triStateName, sharedTriState.getState());
        }
        sharedTriState.addListener(new SharedTriStateListener() {
            @Override
            public void stateHasChanged(SharedTriStateReader var1, SharedTriState.STATE var2) throws Exception {}

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED) {
                    try {
                        reregisterTriState(triStateName, sharedTriStateListeners.get(triStateName), localTriStates.get(triStateName));
                    } catch (Exception e) {
                        log.error("Unable to re-register tri-state {}: {}", triStateName, e.getMessage());
                    }
                }
            }
        });
        sharedTriState.addListener(listener);
    }

    /**
     * Given the shared TriState {@code triStateName}, checks whether the locally cached value matches the expected shared value of {@code expectedValue} . If
     * the value does not match, the local cached value is updated.
     *
     * @param triStateName
     *            the name of the state whose locally cached value is to be tested
     * @param expectedValue
     *            the shared value/expected local value of the state
     * @return {@code true} if the local cached value already matches {@code expectedValue} and {@code false} if not (while at the same time updating the local
     *         cached value to {@code expectedValue}
     */
    public boolean checkTriState(String triStateName, SharedTriState.STATE expectedValue) {
        ArgumentChecker.notNull(triStateName);

        SharedTriState sharedTriState = sharedTriStates.get(triStateName);
        Preconditions.checkArgument(sharedTriState != null, "Invalid TriState name: " + triStateName);
        if (log.isTraceEnabled()) {
            log.trace("table:{} got {} from {}", triStateName, sharedTriState, sharedTriStates);
            log.trace("table:{} checking to see if sharedTriState {} is the same as expected value:{}", triStateName, sharedTriState, expectedValue);
        }
        return sharedTriState.getState() == expectedValue;
    }

    /**
     * Changes the shared distributed triState named {@code triStateName} to the passed value.
     *
     * @param state
     *            the state
     * @param triStateName
     *            the name of the state whose locally cached value is to be tested
     * @throws Exception
     *             if there are issues
     */
    public void setTriState(String triStateName, SharedTriState.STATE state) throws Exception {
        log.trace("table:{} setTriState( {} )", triStateName, state);
        ArgumentChecker.notNull(triStateName);

        SharedTriState sharedTriState = sharedTriStates.get(triStateName);
        Preconditions.checkArgument(sharedTriState != null, "Invalid state name: " + triStateName);
        if (log.isTraceEnabled()) {
            log.trace("table:{} got {} from {}", triStateName, sharedTriState, sharedTriStates);
        }

        if (log.isTraceEnabled()) {
            log.trace("table:{} put( {}, {} )into localTriStates:{}", triStateName, triStateName, state, localTriStates);
        }
        sharedTriState.setState(state);
        localTriStates.put(triStateName, state);
        if (log.isTraceEnabled()) {
            log.trace("table:{} sharedTriState now:{}", triStateName, sharedTriState);
            log.trace("table:{} localTriStates:{}", triStateName, localTriStates);
            log.trace("table:{} sharedTriStates:{}", triStateName, sharedTriStates);
        }
    }

    /**
     * Sends an eviction message for {@code messagePath} to all other shared cache coordinators that are listening.
     *
     * @param messagePath
     *            a message path
     * @throws Exception
     *             if there are issues
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
     * attempt to remove the eviction request marker so as not to overpopulate zookeeper. All servers may try, but only one will actually delete all the nodes.
     *
     * @param callback
     *            the callback to invoke
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
                try {
                    if (curatorClient.checkExists().creatingParentContainersIfNeeded().forPath(responsePath) == null) {
                        curatorClient.create().creatingParentContainersIfNeeded().forPath(responsePath);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    // ignored on purpose -- someone beat us to creating the node
                }
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

    public static class ArgumentChecker {
        private static final String NULL_ARG_MSG = "argument was null";

        public static final void notNull(final Object arg1) {
            if (arg1 == null)
                throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? true");
        }

        public static final void notNull(final Object arg1, final Object arg2) {
            if (arg1 == null || arg2 == null)
                throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null));
        }
    }
}

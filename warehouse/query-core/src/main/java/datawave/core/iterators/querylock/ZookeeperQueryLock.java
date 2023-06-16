package datawave.core.iterators.querylock;

import java.io.File;
import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * Created on 2/6/17. This query lock will create a {@code /query/<queryid>} file upon query start, and will remove it upon query stop. The query is considered
 * running if the file exists, or if we cannot contact zookeeper at the time. A client cleanup interval can be passed in which will automatically release the
 * zookeeper client after the specified interval of non-use.
 */
public class ZookeeperQueryLock implements QueryLock {
    private static Logger log = Logger.getLogger(ZookeeperQueryLock.class);
    private String queryId;
    private String zookeeperConfig;
    private Timer timer = null;
    private long clientCleanupInterval;
    private long lastClientAccess;
    private Lock clientLock = new ReentrantLock();
    private CuratorFramework client = null;
    private DistributedAtomicLong atomicLong = null;

    public ZookeeperQueryLock(String zookeeperConfig, long clientCleanupInterval, String queryId) throws ConfigException {
        this.queryId = queryId;
        this.clientCleanupInterval = clientCleanupInterval;

        URI zookeeperConfigFile = null;
        try {
            zookeeperConfigFile = new Path(zookeeperConfig).toUri();
            if (new File(zookeeperConfigFile).exists()) {
                QuorumPeerConfig zooConfig = new QuorumPeerConfig();
                zooConfig.parse(zookeeperConfigFile.getPath());
                StringBuilder builder = new StringBuilder();
                for (QuorumServer server : zooConfig.getServers().values()) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(server.addr.getReachableOrOne().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                if (builder.length() == 0) {
                    builder.append(zooConfig.getClientPortAddress().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                zookeeperConfig = builder.toString();
            }
        } catch (IllegalArgumentException iae) {
            // ok, try as is
        }
        this.zookeeperConfig = zookeeperConfig;
    }

    private CuratorFramework getClient() {
        if (client == null) {
            clientLock.lock();
            try {
                client = CuratorFrameworkFactory.newClient(zookeeperConfig, 60000, 60000, new RetryNTimes(10, 1000));
                try {
                    client.start();
                    PromotedToLock lock = PromotedToLock.builder().lockPath(getQueryFile() + "/lock").retryPolicy(new RetryNTimes(10, 1000))
                                    .timeout(1, TimeUnit.MINUTES).build();
                    atomicLong = new DistributedAtomicLong(client, getQueryFile(), new RetryNTimes(10, 1000), lock);
                    lastClientAccess = System.currentTimeMillis();
                    if (clientCleanupInterval > 0) {
                        createCleanupTimer();
                    }
                } catch (Exception e) {
                    atomicLong = null;
                    client = null;
                    throw new RuntimeException("Unable to start zookeeper client", e);
                }
            } finally {
                clientLock.unlock();
            }
        }
        return client;
    }

    private DistributedAtomicLong getLong() {
        getClient();
        return atomicLong;
    }

    private void createCleanupTimer() {
        if (timer == null) {
            timer = new Timer("Zookeeper Client Cleanup");
        }
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (lastClientAccess + clientCleanupInterval <= System.currentTimeMillis()) {
                    closeClient();
                    cancel();
                } else if (client == null) {
                    cancel();
                }
            }
        }, clientCleanupInterval, clientCleanupInterval);
    }

    private String getQueryFile() {
        return "/query/" + queryId;
    }

    private void closeClient() {
        if (client != null) {
            clientLock.lock();
            try {
                if (client != null) {
                    try {
                        client.close();
                    } finally {
                        atomicLong = null;
                        client = null;
                    }
                }
            } finally {
                clientLock.unlock();
            }
        }
    }

    private void closeClientAndTimer() {
        if (client != null) {
            clientLock.lock();
            try {
                if (timer != null) {
                    timer.cancel();
                    timer = null;
                }
                if (client != null) {
                    try {
                        client.close();
                    } finally {
                        atomicLong = null;
                        client = null;
                    }
                }
            } finally {
                clientLock.unlock();
            }
        }
    }

    @Override
    public void cleanup() {
        closeClientAndTimer();
    }

    @Override
    public void startQuery() throws Exception {
        // increment the zookeeper query file
        clientLock.lock();
        try {
            DistributedAtomicLong atomicLong = getLong();
            AtomicValue<Long> longValue = atomicLong.increment();
            if (!longValue.succeeded()) {
                throw new ZookeeperLockException("Unable to start query");
            }
            if (log.isTraceEnabled()) {
                long preValue = longValue.preValue();
                long postValue = longValue.postValue();
                log.trace("Updated value for " + getQueryFile() + " from " + preValue + " to " + postValue);
            }
        } finally {
            clientLock.unlock();
        }
    }

    public void stopQuery() throws Exception {
        // decrement the zookeeper query file and delete if 0
        clientLock.lock();
        try {
            DistributedAtomicLong atomicLong = getLong();
            AtomicValue<Long> longValue = atomicLong.decrement();
            if (!longValue.succeeded()) {
                throw new ZookeeperLockException("Unable to stop query");
            }
            long postValue = longValue.postValue();
            if (log.isTraceEnabled()) {
                long preValue = longValue.preValue();
                log.trace("Updated value for " + getQueryFile() + " from " + preValue + " to " + postValue);
            }
            if (postValue == 0) {
                client.delete().forPath(getQueryFile());
            }
        } finally {
            clientLock.unlock();
        }
    }

    @Override
    public boolean isQueryRunning() {
        // check existence of the query file
        clientLock.lock();
        try {
            DistributedAtomicLong atomicLong = getLong();
            AtomicValue<Long> longValue = atomicLong.get();
            if (!longValue.succeeded()) {
                throw new ZookeeperLockException("Unable to get query lock count");
            }
            long postValue = longValue.postValue();
            if (log.isTraceEnabled()) {
                log.trace("Got value for " + getQueryFile() + " to be " + postValue);
            }
            return postValue > 0;
        } catch (Exception e) {
            log.error("Unable to determine if zookeeper lock exists", e);
            // assume the query is still running for now
            return true;
        } finally {
            clientLock.unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        cleanup();
    }

    public static class ZookeeperLockException extends Exception {

        public ZookeeperLockException() {
            super();
        }

        public ZookeeperLockException(String message) {
            super(message);
        }

        public ZookeeperLockException(String message, Throwable cause) {
            super(message, cause);
        }

        public ZookeeperLockException(Throwable cause) {
            super(cause);
        }

        protected ZookeeperLockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

}

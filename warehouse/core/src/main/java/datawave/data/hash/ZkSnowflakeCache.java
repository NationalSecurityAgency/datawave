package datawave.data.hash;

import java.math.BigInteger;

import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class ZkSnowflakeCache {

    private static final Logger LOGGER = Logger.getLogger(ZkSnowflakeCache.class);

    private static CuratorFramework curator;
    private static boolean isInitialized = false;

    public static boolean isInitialized() {
        return isInitialized;
    }

    public static void store(BigInteger machineId, long lastUsedTid) throws Exception {
        if (ZkSnowflakeCache.isInitialized()) {

            String timestampPath = String.format("/snowflake/hosts/%s/timestamp", machineId);

            Stat stat = curator.checkExists().forPath(timestampPath);
            byte[] newTid = LongCombiner.FIXED_LEN_ENCODER.encode(lastUsedTid);

            if (stat == null) {
                curator.create().creatingParentContainersIfNeeded().forPath(timestampPath, newTid);
            } else {

                curator.setData().forPath(timestampPath, newTid);

            }
        } else {
            LOGGER.error("ZkSnowflakeCache was not initialized");
            throw new RuntimeException("ZkSnowflakeCache was not initialized");
        }

    }

    public static long getLastCachedTid(BigInteger machineId) throws Exception {
        long oldTid = 0;

        String timestampPath = String.format("/snowflake/hosts/%s/timestamp", machineId);

        Stat stat = curator.checkExists().forPath(timestampPath);

        if (stat == null) {
            curator.create().creatingParentContainersIfNeeded().forPath(timestampPath);
        } else {
            byte[] data = curator.getData().forPath(timestampPath);
            oldTid = LongCombiner.FIXED_LEN_ENCODER.decode(data);
        }
        return oldTid;
    }

    public static synchronized void init(String zks, int retries, int sleepMillis) {

        if (!isInitialized) {
            RetryPolicy retryPolicy = new RetryNTimes(retries, sleepMillis);
            curator = CuratorFrameworkFactory.newClient(zks, retryPolicy);
            curator.start();

            isInitialized = true;
        }
    }

    public static synchronized void stop() {

        curator.close();
        isInitialized = false;

    }

}

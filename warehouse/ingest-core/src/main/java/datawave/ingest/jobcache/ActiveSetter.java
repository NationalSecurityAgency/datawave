package datawave.ingest.jobcache;

import java.nio.charset.Charset;

import org.apache.curator.framework.CuratorFramework;

/**
 * Sets the active job cache.
 */
public class ActiveSetter {

    private final CuratorFramework zkClient;

    public ActiveSetter(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * Sets the active job cache in Zookeeper.
     *
     * @param path
     *            The ZK node to set
     * @param activeJobCache
     *            The active job cache
     * @throws Exception
     *             if the operation does not succeed
     */
    public void set(String path, String activeJobCache) throws Exception {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be empty");
        }
        if (activeJobCache == null || activeJobCache.isEmpty()) {
            throw new IllegalArgumentException("activeJobCache cannot be empty");
        }

        if (!zkPathExists(path)) {
            createZkPath(path);
        }

        updateZkPath(path, activeJobCache);
    }

    private boolean zkPathExists(String path) throws Exception {
        return zkClient.checkExists().forPath(path) != null;
    }

    private void createZkPath(String path) throws Exception {
        zkClient.create().creatingParentsIfNeeded().forPath(path);
    }

    private void updateZkPath(String path, String value) throws Exception {
        zkClient.setData().forPath(path, value.getBytes(Charset.defaultCharset()));
    }
}

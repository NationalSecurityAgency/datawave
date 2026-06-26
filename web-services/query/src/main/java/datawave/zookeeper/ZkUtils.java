package datawave.zookeeper;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public final class ZkUtils {

    /**
     * Extract the zookeeper connect string from the given string.
     *
     * @param zookeeperConfig
     *            the zookeeper connect string, or a path to a zookeeper config file
     * @return the connect string
     * @throws QuorumPeerConfig.ConfigException
     *             if a valid connect string could not be extracted from the config file
     */
    public static String getQuorumPeerConfig(String zookeeperConfig) throws QuorumPeerConfig.ConfigException {
        URI zookeeperConfigFile;
        try {
            zookeeperConfigFile = new Path(zookeeperConfig).toUri();
            if (new File(zookeeperConfigFile).exists()) {
                QuorumPeerConfig zooConfig = new QuorumPeerConfig();
                zooConfig.parse(zookeeperConfigFile.getPath());
                StringBuilder sb = new StringBuilder();
                for (QuorumPeer.QuorumServer server : zooConfig.getServers().values()) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(server.addr.getReachableOrOne().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                if (sb.length() == 0) {
                    sb.append(zooConfig.getClientPortAddress().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                return sb.toString();
            }
        } catch (IllegalArgumentException e) {
            // Try the zookeeper config as is.
        }
        return zookeeperConfig;
    }

    private ZkUtils() {
        throw new UnsupportedOperationException();
    }
}

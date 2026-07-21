package datawave.zookeeper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * Utility class for Zookeeper operations.
 */
public final class ZkUtils {

    /**
     * Return a formatted Zookeeper connect string that can be used to connect to a running Zookeeper server. The config string can be a list of servers or a
     * path to a Zookeeper config file.
     *
     * @param config
     *            the configuration file/string
     * @return the configuration
     * @throws QuorumPeerConfig.ConfigException
     *             if the argument is a file that cannot be parsed as a zookeeper config file
     */
    public static String getQuorumPeerConfig(String config) throws QuorumPeerConfig.ConfigException {
        URI zookeeperConfigFile;
        try {
            URI uri = new URI(config);
            // Create the path differently depending on whether the config is a filepath with a URI scheme or not. This is important to avoid errors when trying
            // to determine if the config points to a file.
            Path path = uri.getScheme() != null ? Paths.get(uri) : Paths.get(config);
            if (!Files.isRegularFile(path)) {
                return config;
            }
            zookeeperConfigFile = uri;
        } catch (Exception e) {
            // The config argument does not point to an existing file. Try it as is.
            return config;
        }

        // If the config points to an existing file, attempt to parse it as a zookeeper config file.
        QuorumPeerConfig zooConfig = new QuorumPeerConfig();
        zooConfig.parse(zookeeperConfigFile.getPath());
        StringBuilder sb = new StringBuilder();

        int port = zooConfig.getClientPortAddress().getPort();

        // If there are any servers in the config, add their client addresses.
        for (QuorumPeer.QuorumServer server : zooConfig.getServers().values()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(server.addr.getReachableOrOne().getHostName()).append(':').append(port);
        }

        // If no server addresses were added, use the hostname of the client port address.
        if (sb.length() == 0) {
            sb.append(zooConfig.getClientPortAddress().getHostName()).append(':').append(port);
        }
        return sb.toString();
    }

    /**
     * Do not allow this class to be instantiated.
     */
    private ZkUtils() {
        throw new UnsupportedOperationException();
    }
}

package datawave.zookeeper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Utility class for Zookeeper operations.
 */
public final class ZkUtils {

    private static final Cache<String,String> zkConfigCache = Caffeine.newBuilder().maximumSize(16).build();

    /**
     * Return a formatted Zookeeper connect string that can be used to connect to a running Zookeeper server. The config string can be a list of servers or a
     * path to a Zookeeper config file. If a valid connect string is already cached for the given config, the cached connect string will be returned.
     *
     * @param config
     *            the configuration file/string
     * @return the configuration
     * @throws QuorumPeerConfig.ConfigException
     *             if the argument is a file that cannot be parsed as a zookeeper config file
     */
    public static String getQuorumPeerConfig(String config) throws QuorumPeerConfig.ConfigException {
        String connectString = zkConfigCache.getIfPresent(config);
        if (connectString == null) {
            connectString = extractQuorumPeerConfig(config);
            zkConfigCache.put(config, connectString);
        }
        return connectString;
    }

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
    private static String extractQuorumPeerConfig(String config) throws QuorumPeerConfig.ConfigException {
        URI zookeeperConfigFile;
        try {
            URI uri = new URI(config);
            // Create the path differently depending on whether the config is a filepath with a URI scheme or not. This is important to avoid errors when trying
            // to determine if the config points to a file.
            Path path = uri.getScheme() != null ? Paths.get(uri) : Paths.get(config);
            if (!Files.isRegularFile(path)) {
                zkConfigCache.put(config, config);
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
     * Normalizes the given path. Returns an empty string if the path is null or blank. The following changes will be applied:
     * <ul>
     * <li>The path will be trimmed.</li>
     * <li>Consecutive slashes will be collapsed.</li>
     * <li>A leading slash will be added if one is not present.</li>
     * <li>Trailing slashes will be removed.</li>
     * </ul>
     *
     * @param path
     *            the path to normalize
     * @return the normalized path
     */
    public static String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        path = path.trim();
        path = path.replaceAll("/{2,}", "/");
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * Do not allow this class to be instantiated.
     */
    private ZkUtils() {
        throw new UnsupportedOperationException();
    }
}

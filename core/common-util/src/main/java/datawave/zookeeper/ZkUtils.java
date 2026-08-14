package datawave.zookeeper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.regex.Pattern;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;

/**
 * Utility class for Zookeeper operations.
 */
public final class ZkUtils {

    // @formatter:off
    private static final Cache<String,String> zkConfigCache = Caffeine.newBuilder()
                    .maximumSize(16)
                    .expireAfterWrite(Duration.ofMinutes(10))
                    .build();
    // @formatter:on

    private static final Pattern CONSECUTIVE_SLASH_REGEX = Pattern.compile("/{2,}");

    /**
     * Return a formatted Zookeeper connect string that can be used to connect to a running Zookeeper server. If the string is a path to a Zookeeper
     * configuration file, the connect string will be resolved using {@link QuorumPeerConfig}. If a connect string is already cached for the given config
     * argument, the cached connect string will be returned.
     *
     * @param config
     *            the configuration file/string
     * @return the configuration
     * @throws NullPointerException
     *             if the configuration s
     * @throws IllegalArgumentException
     *             if config is null or blank, or is a path to a Zookeeper configuration file that could not be parsed with {@link QuorumPeerConfig}
     */
    public static String getConnectString(String config) {
        Preconditions.checkArgument(config != null && !config.isBlank(), "config must not be null or blank");
        String connectString = zkConfigCache.getIfPresent(config);
        if (connectString == null) {
            try {
                connectString = parseQuorumPeerConfig(config);
            } catch (QuorumPeerConfig.ConfigException e) {
                throw new IllegalArgumentException("Unable to parse quorum peer config: " + config, e);
            }
            if (connectString != null) {
                zkConfigCache.put(config, connectString);
            }
        }
        return connectString;
    }

    /**
     * Return a formatted Zookeeper connect string that can be used to connect to a running Zookeeper server. The config string can be a list of servers or a
     * path to a Zookeeper config file.
     *
     * @param configStr
     *            the configuration file/string
     * @return the configuration
     * @throws QuorumPeerConfig.ConfigException
     *             if the argument is a file that cannot be parsed as a zookeeper config file
     */
    private static String parseQuorumPeerConfig(String configStr) throws QuorumPeerConfig.ConfigException {
        Path path;
        try {
            URI uri = new URI(configStr);
            // Create the path differently depending on whether the config is a filepath with a URI scheme or not. This is important to avoid errors when trying
            // to determine if the config points to a file.
            path = uri.getScheme() != null ? Paths.get(uri) : Paths.get(configStr);
            if (!Files.isRegularFile(path)) {
                return configStr;
            }
        } catch (Exception e) {
            // The config argument does not point to an existing file. Try it as is.
            return configStr;
        }

        // If the config points to an existing file, attempt to parse it as a zookeeper config file.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(path.toString());
        StringBuilder sb = new StringBuilder();

        int port = config.getClientPortAddress().getPort();

        // If there are any servers in the config, add their client addresses.
        for (QuorumPeer.QuorumServer server : config.getServers().values()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(server.addr.getReachableOrOne().getHostName()).append(':').append(port);
        }

        // If no server addresses were added, use the hostname of the client port address.
        if (sb.length() == 0) {
            sb.append(config.getClientPortAddress().getHostName()).append(':').append(port);
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
        path = CONSECUTIVE_SLASH_REGEX.matcher(path).replaceAll("/");
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

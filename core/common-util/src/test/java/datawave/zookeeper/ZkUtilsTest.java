package datawave.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetSystemProperty;

class ZkUtilsTest {

    /**
     * Tests for {@link ZkUtils#getQuorumPeerConfig(String)}.
     */
    @Nested
    class GetQuorumPeerConfigTests {

        @TempDir
        Path tempDir;

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a non-filepath argument, the original argument is returned.
         */
        @Test
        void testPlainConnectStringIsReturnedAsIs() throws QuorumPeerConfig.ConfigException {
            assertEquals("localhost:2181", ZkUtils.getQuorumPeerConfig("localhost:2181"));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a filepath that does not point to an existing file, the original argument is
         * returned.
         */
        @Test
        void testNonExistentFilePathIsReturnedAsIs() throws QuorumPeerConfig.ConfigException {
            String config = tempDir.resolve("non-existent.cfg").toString();
            assertEquals(config, ZkUtils.getQuorumPeerConfig(config));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a directory path, the original argument is returned.
         */
        @Test
        void testDirectoryPathIsReturnedAsIs() throws QuorumPeerConfig.ConfigException {
            String config = tempDir.toString();
            assertEquals(config, ZkUtils.getQuorumPeerConfig(config));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given invalid URI syntax, the original argument is returned.
         */
        @Test
        void testInvalidURISyntax() throws QuorumPeerConfig.ConfigException {
            String config = "not a valid : uri string";
            assertEquals(config, ZkUtils.getQuorumPeerConfig(config));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given invalid zookeeper config file with invalid contents, an exception is thrown.
         */
        @Test
        void testInvalidConfigFileContentsThrowsException() throws IOException {
            Properties properties = new Properties();
            properties.put("tickTime", "2000");

            String path = createZookeeperCfgFile(properties);
            assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZkUtils.getQuorumPeerConfig(path));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config path with the URI scheme {@code file://}, it is able
         * to load and read the file.
         */
        @Test
        void testFileSchemePointingToConfigFile() throws QuorumPeerConfig.ConfigException, IOException {
            Properties properties = new Properties();
            properties.put("tickTime", "2000");
            properties.put("dataDir", "/var/zookeeper");
            properties.put("initLimit", "2");
            properties.put("syncLimit", "5");
            properties.put("clientPort", "2181");

            String path = createZookeeperCfgFile(properties);
            assertEquals("0.0.0.0:2181", ZkUtils.getQuorumPeerConfig("file://" + path));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config without a client port address, the default client port
         * address {@code 0.0.0.0} is returned.
         */
        @Test
        void testConfigFileWithoutClientPortAddressReturnsDefaultClientPortAddress() throws QuorumPeerConfig.ConfigException, IOException {
            Properties properties = new Properties();
            properties.put("tickTime", "2000");
            properties.put("dataDir", "/var/zookeeper");
            properties.put("initLimit", "2");
            properties.put("syncLimit", "5");
            properties.put("clientPort", "2181");

            String path = createZookeeperCfgFile(properties);
            assertEquals("0.0.0.0:2181", ZkUtils.getQuorumPeerConfig(path));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config without servers, the default client port address is
         * returned.
         */
        @Test
        void testConfigFileWithoutServersReturnsDefaultClientPortAddress() throws QuorumPeerConfig.ConfigException, IOException {
            Properties properties = new Properties();
            properties.put("tickTime", "2000");
            properties.put("dataDir", "/var/zookeeper");
            properties.put("initLimit", "2");
            properties.put("syncLimit", "5");
            properties.put("clientPort", "2181");
            properties.put("clientPortAddress", "192.168.1.50");

            String path = createZookeeperCfgFile(properties);
            InetSocketAddress clientSocketAddress = new InetSocketAddress(InetAddress.getByName("192.168.1.50"), 2181);
            String expected = clientSocketAddress.getHostName() + ":2181";

            assertEquals(expected, ZkUtils.getQuorumPeerConfig(path));
        }

        /**
         * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config with servers, a comma-delimited list of the server
         * client port addresses are returned.
         */
        @SetSystemProperty(key = QuorumPeer.CONFIG_KEY_MULTI_ADDRESS_ENABLED, value = "true")
        @Test
        void testConfigFileWithServersReturnsServerClientPortAddresses() throws QuorumPeerConfig.ConfigException, IOException {
            // In order to not trigger a QuorumPeer exception, we need to create a myid file with one of the server IDs in it.
            Path myidPath = tempDir.resolve("myid");
            Files.write(myidPath, "1".getBytes(StandardCharsets.UTF_8));

            // Make the dataDir property point to the temp dir so that QuorumPeer can find the myid file.
            Properties properties = new Properties();
            properties.put("tickTime", "2000");
            properties.put("dataDir", "/var/zookeeper");
            properties.put("initLimit", "2");
            properties.put("syncLimit", "5");
            properties.put("clientPort", "2181");
            properties.put("server.1", "zoo1:2888:3888|client1:2888:3888");
            properties.put("server.2", "zoo2:2888:3888|client2:2888:3888");
            properties.setProperty("dataDir", tempDir.toString());

            String path = createZookeeperCfgFile(properties);
            assertEquals("client1:2181,client2:2181", ZkUtils.getQuorumPeerConfig(path));
        }

        private String createZookeeperCfgFile(Properties properties) throws IOException {
            Path configPath = tempDir.resolve("zookeeper.cfg");
            properties.store(new FileOutputStream(configPath.toFile()), null);
            return configPath.toAbsolutePath().toString();
        }
    }

    /**
     * Tests for {@link ZkUtils#normalizePath(String)}.
     */
    @Nested
    class NormalizePathTests {

        /**
         * Verify that an empty string is returned given a null path.
         */
        @Test
        void nullPathReturnsEmptyString() {
            assertEquals("", ZkUtils.normalizePath(null));
        }

        /**
         * Verify that an empty string is returned given a blank path.
         */
        @Test
        void blankPathReturnsEmptyString() {
            assertEquals("", ZkUtils.normalizePath("   "));
        }

        /**
         * Verify that the path is trimmed.
         */
        @Test
        void pathIsTrimmed() {
            assertEquals("/foo/bar", ZkUtils.normalizePath(" /foo/bar "));
        }

        /**
         * Verify that a leading slash is added if missing.
         */
        @Test
        void leadingSlashIsAdded() {
            assertEquals("/foo/bar", ZkUtils.normalizePath("foo/bar"));
        }

        /**
         * Verify that trailing slashes are removed.
         */
        @Test
        void trailingSlashIsRemoved() {
            assertEquals("/foo/bar", ZkUtils.normalizePath("foo/bar/"));
        }

        /**
         * Verify that consecutive slashes are collapsed.
         */
        @Test
        void consecutiveSlashesAreCollapsed() {
            assertEquals("/foo/bar", ZkUtils.normalizePath("///foo////bar///"));
        }
    }
}

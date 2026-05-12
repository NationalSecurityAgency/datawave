package datawave.webservice.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetSystemProperty;

class ZkUtilsTest {

    @TempDir
    File tempDir;

    /**
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a non-filepath argument, the original argument is returned.
     */
    @Test
    void testGetQuorumPeerConfigGivenNonFilePath() throws QuorumPeerConfig.ConfigException {
        assertEquals("localhost:2181", ZkUtils.getQuorumPeerConfig("localhost:2181"));
    }

    /**
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a filepath that does not point to an existing file, the original argument is
     * returned.
     */
    @Test
    void testGetQuorumPeerConfigGivenNonExistentFile() throws QuorumPeerConfig.ConfigException {
        assertEquals("/i/do/not/exist/zookeeper.cfg", ZkUtils.getQuorumPeerConfig("/i/do/not/exist/zookeeper.cfg"));
    }

    /**
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given an invalid zookeeper config file, an exception is thrown.
     */
    @Test
    void testGetQuorumPeerConfigGivenInvalidConfigFile() throws IOException {
        Properties properties = new Properties();
        properties.put("tickTime", "2000");

        String path = createZookeeperCfgFile(properties);
        assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZkUtils.getQuorumPeerConfig(path));
    }

    /**
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config path with the URI scheme {@code file://}, it is able to
     * load and read the file.
     */
    @Test
    void testGetQuorumPeerConfigGivenPathWithLocalFileScheme() throws QuorumPeerConfig.ConfigException, IOException {
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
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config a client port address, the default client port address
     * {@code 0.0.0.0} is returned.
     */
    @Test
    void testGetQuorumPeerConfigGivenValidConfigFileWithoutClientPortAddress() throws QuorumPeerConfig.ConfigException, IOException {
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
    void testGetQuorumPeerConfigGivenValidConfigFileWithoutServers() throws QuorumPeerConfig.ConfigException, IOException {
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
     * Verify that when {@link ZkUtils#getQuorumPeerConfig(String)} is given a valid zookeeper config with servers, a comma-delimited list of the server client
     * port addresses are returned.
     */
    @SetSystemProperty(key = QuorumPeer.CONFIG_KEY_MULTI_ADDRESS_ENABLED, value = "true")
    @Test
    void testGetQuorumPeerConfigGivenValidConfigFileWithServers() throws QuorumPeerConfig.ConfigException, IOException {
        // In order to not trigger a QuorumPeer exception, we need to create a myid file with one of the server IDs in it.
        File myidFile = new File(tempDir, "myid");
        Files.writeString(myidFile.toPath(), "1");

        // Make the dataDir property point to the temp dir so that QuorumPeer can find the myid file.
        Properties properties = new Properties();
        properties.put("tickTime", "2000");
        properties.put("dataDir", "/var/zookeeper");
        properties.put("initLimit", "2");
        properties.put("syncLimit", "5");
        properties.put("clientPort", "2181");
        properties.put("server.1", "zoo1:2888:3888|client1:2888:3888");
        properties.put("server.2", "zoo2:2888:3888|client2:2888:3888");
        properties.setProperty("dataDir", tempDir.getAbsolutePath());

        String path = createZookeeperCfgFile(properties);
        assertEquals("client1:2181,client2:2181", ZkUtils.getQuorumPeerConfig(path));
    }

    private String createZookeeperCfgFile(Properties properties) throws IOException {
        File file = new File(tempDir, "zookeeper.cfg");
        properties.store(new FileOutputStream(file), null);
        return file.getAbsolutePath();
    }
}

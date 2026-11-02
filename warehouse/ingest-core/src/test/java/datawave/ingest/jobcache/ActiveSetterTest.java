package datawave.ingest.jobcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ActiveSetterTest {

    private static final String ACTIVE_PATH = "/datawave/activeJobCache";

    private static TestingServer ZK_SERVER;
    private static CuratorFramework ZK_CLIENT;

    private ActiveSetter activeSetter;

    @BeforeAll
    public static void setupAll() throws Exception {
        ZK_SERVER = new TestingServer();
        ZK_SERVER.start();

        ZK_CLIENT = CuratorFrameworkFactory.newClient(ZK_SERVER.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        ZK_CLIENT.start();
    }

    @BeforeEach
    public void setup() {
        activeSetter = new ActiveSetter(ZK_CLIENT);
    }

    @Test
    public void shouldSetActive() throws Exception {
        String activeJobCache = "/data/jobCacheA";

        activeSetter.set(ACTIVE_PATH, activeJobCache);

        assertZkData(ACTIVE_PATH, activeJobCache);
    }

    @Test
    public void shouldOverwrite() throws Exception {
        String activeJobCache1 = "/data/jobCacheA";
        String activeJobCache2 = "/data/jobCacheB";

        activeSetter.set(ACTIVE_PATH, activeJobCache1);
        activeSetter.set(ACTIVE_PATH, activeJobCache2);

        assertZkData(ACTIVE_PATH, activeJobCache2);
    }

    @Test
    public void shouldNotAcceptEmptyPath() throws Exception {
        String emptyJobCache = "";

        assertThrows(IllegalArgumentException.class, () -> activeSetter.set(ACTIVE_PATH, emptyJobCache));
    }

    private void assertZkData(String path, String expectedValue) throws Exception {
        String actualValue = new String(ZK_CLIENT.getData().forPath(path), Charset.defaultCharset());

        assertEquals(expectedValue, actualValue);
    }

    @AfterAll
    public static void tearDownAll() throws IOException {
        if (ZK_CLIENT != null) {
            ZK_CLIENT.close();
        }

        if (ZK_SERVER != null) {
            ZK_SERVER.stop();
        }
    }
}

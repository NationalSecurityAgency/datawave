package datawave.ingest.jobcache;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActiveSetterTest {

    private static final String ACTIVE_PATH = "/datawave/activeJobCache";

    private static TestingServer ZK_SERVER;
    private static CuratorFramework ZK_CLIENT;

    private ActiveSetter activeSetter;

    @BeforeClass
    public static void setupAll() throws Exception {
        ZK_SERVER = new TestingServer();
        ZK_SERVER.start();

        ZK_CLIENT = CuratorFrameworkFactory.newClient(ZK_SERVER.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        ZK_CLIENT.start();
    }

    @Before
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

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptEmptyPath() throws Exception {
        String emptyJobCache = "";

        activeSetter.set(ACTIVE_PATH, emptyJobCache);
    }

    private void assertZkData(String path, String expectedValue) throws Exception {
        String actualValue = new String(ZK_CLIENT.getData().forPath(path), Charset.defaultCharset());

        assertEquals(expectedValue, actualValue);
    }

    @AfterClass
    public static void tearDownAll() throws IOException {
        if (ZK_CLIENT != null) {
            ZK_CLIENT.close();
        }

        if (ZK_SERVER != null) {
            ZK_SERVER.stop();
        }
    }
}

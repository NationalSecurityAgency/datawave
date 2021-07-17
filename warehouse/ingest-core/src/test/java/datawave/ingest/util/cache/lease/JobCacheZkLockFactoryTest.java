package datawave.ingest.util.cache.lease;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class JobCacheZkLockFactoryTest {
    private static final String TIMESTAMP_ID = "timestamp";
    private static final String ZNODE_JOB_ID_1 = TIMESTAMP_ID + "/jobId1";
    private static final String ZNODE_JOB_ID_2 = TIMESTAMP_ID + "/jobId2";
    private static final String ZOOKEEPER_LEASE_DIR = "/leases";
    private static final String ZOOKEEPER_CACHE_NAMESPACE = "test/jobCache";
    
    private static final int ZOOKEEPER_LOCK_TIMEOUT_MS = 30;
    private static final int ZOOKEEPER_RETRY_TIMEOUT_MS = 30;
    private static final int ZOOKEEPER_RETRY_CNT = 3;
    
    private static String zookeepers;
    private static TestingServer testingServer;
    
    private CuratorFramework client;
    private JobCacheZkLockFactory lockFactory;
    
    @BeforeClass
    public static void setupClass() throws Exception {
        InstanceSpec instanceSpec = InstanceSpec.newInstanceSpec();
        testingServer = new TestingServer(instanceSpec, true);
        zookeepers = instanceSpec.getConnectString();
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        testingServer.close();
    }
    
    @Before
    public void setup() {
        lockFactory = getNewLockFactory();
        client = lockFactory.getCurator();
    }
    
    @After
    public void tearDown() {
        lockFactory.close();
    }
    
    /*
     * Extra: Test setup features like retry timeout and locking.
     */
    
    @Test
    public void shouldAcquireLock() throws Exception {
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_1));
        verifyLockFileWasCreated(ZNODE_JOB_ID_1);
    }
    
    @Test
    public void shouldNotAcquireLockDueToCacheLocked() {
        Assert.assertTrue(lockFactory.acquireLock(TIMESTAMP_ID));
        Assert.assertFalse(lockFactory.acquireLock(ZNODE_JOB_ID_1));
    }
    
    @Test
    public void shouldAcquireJobIdLockWhenCacheIsActive() {
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_1));
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_2));
    }
    
    @Test
    public void shouldNotAcquireCacheLockWhenCacheIsActive() {
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_1));
        Assert.assertFalse(lockFactory.acquireLock(TIMESTAMP_ID, lockFactory.getCacheAvailablePredicate()));
    }
    
    @Test
    public void shouldReleaseLock() throws Exception {
        try (JobCacheZkLockFactory secondaryLockFactory = getNewLockFactory()) {
            
            Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_1));
            Assert.assertFalse(secondaryLockFactory.acquireLock(ZNODE_JOB_ID_1));
            
            Assert.assertTrue(lockFactory.releaseLock(ZNODE_JOB_ID_1));
            Assert.assertTrue(secondaryLockFactory.acquireLock(ZNODE_JOB_ID_1));
        }
        verifyLockFileWasReleased(ZNODE_JOB_ID_1);
        
    }
    
    @Test
    public void shouldCleanupReleasedZookeeperNode() throws Exception {
        try (JobCacheZkLockFactory secondaryLockFactory = getNewLockFactory()) {
            Assert.assertTrue(secondaryLockFactory.acquireLock(ZNODE_JOB_ID_2));
            verifyLockFileWasCreated(ZNODE_JOB_ID_2);
            Assert.assertTrue(secondaryLockFactory.releaseLock(ZNODE_JOB_ID_2));
        }
        
        verifyLockFileWasReleased(ZNODE_JOB_ID_2);
    }
    
    @Test
    public void shouldShowActiveCacheWithTwoJobIds() {
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_1));
        Assert.assertTrue(lockFactory.acquireLock(ZNODE_JOB_ID_2));
        LockCacheStatus status = lockFactory.getCacheStatus(TIMESTAMP_ID);
        Assert.assertTrue(status.isCacheActive());
        Assert.assertFalse(status.isCacheLocked());
        Assert.assertTrue(status.getJobIds().contains(ZNODE_JOB_ID_1));
        Assert.assertTrue(status.getJobIds().contains(ZNODE_JOB_ID_2));
    }
    
    @Test
    public void shouldShowInActiveCacheWithNoJobIds() {
        LockCacheStatus status = lockFactory.getCacheStatus(ZNODE_JOB_ID_1);
        Assert.assertFalse(status.isCacheActive());
        Assert.assertFalse(status.isCacheLocked());
        Assert.assertEquals(status.getJobIds().size(), 0);
    }
    
    @Test
    public void shouldShowLockedCacheWithNoJobIds() {
        Assert.assertTrue(lockFactory.acquireLock(TIMESTAMP_ID));
        LockCacheStatus status = lockFactory.getCacheStatus(TIMESTAMP_ID);
        Assert.assertFalse(status.isCacheActive());
        Assert.assertTrue(status.isCacheLocked());
        Assert.assertEquals(status.getJobIds().size(), 0);
    }
    
    private JobCacheZkLockFactory getNewLockFactory() {
        return new JobCacheZkLockFactory(ZOOKEEPER_CACHE_NAMESPACE, zookeepers, ZOOKEEPER_LOCK_TIMEOUT_MS, ZOOKEEPER_RETRY_CNT, ZOOKEEPER_RETRY_TIMEOUT_MS);
    }
    
    private void verifyLockFileWasCreated(String id) throws Exception {
        Assert.assertEquals(client.checkExists().forPath(lockFactory.getLockPath(id) + ZOOKEEPER_LEASE_DIR).getNumChildren(), 1);
    }
    
    private void verifyLockFileWasReleased(String id) throws Exception {
        Assert.assertNull(client.checkExists().forPath(lockFactory.getLockPath(id)));
    }
}

package datawave.ingest.util.cache;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import datawave.common.test.utils.FileUtils;
import datawave.ingest.util.cache.lease.JobCacheZkLockFactory;
import datawave.ingest.util.cache.lease.LockCacheStatus;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;

public class JobCacheFactoryTest {
    private static final String TEMP_DIR = Files.createTempDir().getAbsolutePath();
    private static final String ZOOKEEPER_CACHE_NAMESPACE = "test/jobCache";
    private static final String JOB_CACHE_TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS";
    private static final DateTimeFormatter JOB_CACHE_FORMATER = DateTimeFormatter.ofPattern(JOB_CACHE_TIMESTAMP_FORMAT);
    private static final String JOB_CACHE_PREFIX = "jobCache_";
    private static final Pattern JOB_CACHE_PATTERN = Pattern.compile(JOB_CACHE_PREFIX + "\\d{" + JOB_CACHE_TIMESTAMP_FORMAT.length() + "}");
    private static final String TEST_JOB_ID1 = "testJobId1";
    private static final String TEST_JOB_ID2 = "testJobId2";
    
    private static TestingServer TESTING_SERVER;
    private static String TEST_ZOOKEEPERS;
    private static String CACHE_TIMESTAMP_A;
    private static String CACHE_TIMESTAMP_B;
    private static String CACHE_TIMESTAMP_C;
    private static String CACHE_TIMESTAMP_D;
    
    @BeforeClass
    public static void setupClass() throws Exception {
        InstanceSpec instanceSpec = InstanceSpec.newInstanceSpec();
        TESTING_SERVER = new TestingServer(instanceSpec, true);
        TEST_ZOOKEEPERS = instanceSpec.getConnectString();
        CACHE_TIMESTAMP_A = getTimestampPath();
        CACHE_TIMESTAMP_B = getTimestampPath();
        CACHE_TIMESTAMP_C = getTimestampPath();
        CACHE_TIMESTAMP_D = getTimestampPath();
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        TESTING_SERVER.close();
        FileUtils.deletePath(TEMP_DIR);
    }
    
    private static String getTimestampPath() throws IOException, InterruptedException {
        // @formatter:off
        String jarFile =  new StringJoiner(File.separator)
                .add(TEMP_DIR)
                .add(JOB_CACHE_PREFIX+JOB_CACHE_FORMATER.format(LocalDateTime.now()))
                .add("File.jar")
                .toString();
        // @formatter:on
        
        TimeUnit.MILLISECONDS.sleep(10);
        return createTemporaryFile(new File(jarFile)).getParent();
    }
    
    private JobCacheFactory jobCacheFactory;
    private Configuration conf;
    
    @Before
    public void setup() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        conf = new Configuration();
        conf.set(JobCacheFactory.DW_JOB_CACHE_BASE_PATH, TEMP_DIR);
        conf.set(JobCacheFactory.DW_JOB_CACHE_LOCK_FACTORY, JobCacheZkLockFactory.class.getName());
        conf.set(JobCacheFactory.DW_JOB_CACHE_TIMESTAMP_DIR_PREFIX, JOB_CACHE_PREFIX);
        conf.set(JobCacheFactory.DW_JOB_CACHE_TIMESTAMP_PATTERN, JOB_CACHE_PATTERN.pattern());
        
        conf.set(JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_TIMEOUT, "30");
        conf.set(JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_RETRY_CNT, "3");
        conf.set(JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_RETRY_TIMEOUT, "30");
        conf.set(JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_NODES, TEST_ZOOKEEPERS);
        conf.set(JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_NAMESPACE, ZOOKEEPER_CACHE_NAMESPACE);
        
        jobCacheFactory = new JobCacheFactory(conf);
    }
    
    @After
    public void tearDown() {
        jobCacheFactory.close();
    }
    
    @Test
    public void shouldFindLatestAvailableCache() throws Exception {
        Path expectedJobCache = new Path(CACHE_TIMESTAMP_B);
        
        try (JobCacheZkLockFactory lockFactory = new JobCacheZkLockFactory()) {
            lockFactory.init(conf);
            acquireLock(lockFactory, "", CACHE_TIMESTAMP_C, CACHE_TIMESTAMP_D);
            Path jobCache = jobCacheFactory.getJobCache(TEST_JOB_ID1);
            Assert.assertEquals(expectedJobCache.getName(), jobCache.getName());
        }
    }
    
    @Test(expected = Exception.class)
    public void shouldThrowExceptionWhenNoCacheIsAvailable() throws Exception {
        try (JobCacheZkLockFactory lockFactory = new JobCacheZkLockFactory()) {
            lockFactory.init(conf);
            acquireLock(lockFactory, "", CACHE_TIMESTAMP_A, CACHE_TIMESTAMP_B, CACHE_TIMESTAMP_C, CACHE_TIMESTAMP_D);
            jobCacheFactory.getJobCache(TEST_JOB_ID1);
        }
    }
    
    @Test
    public void shouldReleaseLock() throws Exception {
        try (JobCacheZkLockFactory lockFactory = new JobCacheZkLockFactory()) {
            lockFactory.init(conf);
            
            Path jobCache = jobCacheFactory.getJobCache(TEST_JOB_ID1);
            LockCacheStatus status = lockFactory.getCacheStatus(jobCache.getName());
            Assert.assertTrue(status.isCacheActive());
            Assert.assertTrue(status.getJobIds().contains(getLockPath(jobCache.getName(), TEST_JOB_ID1)));
            
            Assert.assertTrue(jobCacheFactory.releaseId(TEST_JOB_ID1));
            status = lockFactory.getCacheStatus(jobCache.getName());
            Assert.assertFalse(status.isCacheActive());
            Assert.assertFalse(status.getJobIds().contains(getLockPath(jobCache.getName(), TEST_JOB_ID1)));
        }
    }
    
    @Test
    public void shouldReportStatusOnAllCaches() throws IOException {
        Collection<JobCacheStatus> expectedStatuses = Lists.newArrayList();
        expectedStatuses.add(getCacheStatus(CACHE_TIMESTAMP_D, false, TEST_JOB_ID2));
        expectedStatuses.add(getCacheStatus(CACHE_TIMESTAMP_C, true));
        expectedStatuses.add(getCacheStatus(CACHE_TIMESTAMP_B, false));
        expectedStatuses.add(getCacheStatus(CACHE_TIMESTAMP_A, false, TEST_JOB_ID1, TEST_JOB_ID2));
        
        try (JobCacheZkLockFactory lockFactory = new JobCacheZkLockFactory()) {
            lockFactory.init(conf);
            
            acquireLock(lockFactory, TEST_JOB_ID1, CACHE_TIMESTAMP_A);
            acquireLock(lockFactory, TEST_JOB_ID2, CACHE_TIMESTAMP_A);
            acquireLock(lockFactory, "", CACHE_TIMESTAMP_C);
            acquireLock(lockFactory, TEST_JOB_ID2, CACHE_TIMESTAMP_D);
            
            FileSystemPath baseCachePath = new FileSystemPath(FileSystem.getLocal(new Configuration()), new Path(TEMP_DIR));
            Collection<JobCacheStatus> cacheStatuses = jobCacheFactory.getJobCacheStatuses(baseCachePath);
            
            Assert.assertEquals(expectedStatuses, cacheStatuses);
        }
    }
    
    private void acquireLock(JobCacheZkLockFactory lockFactory, String jobId, String... caches) {
        // @formatter:off
        Arrays.stream(caches)
                .map(this::getTimestampName)
                .map(cacheName -> getLockPath(cacheName, jobId))
                .forEach(lockFactory::acquireLock);
        // @formatter:on
    }
    
    private JobCacheStatus getCacheStatus(String timestampPath, boolean locked, String... jobIds) throws IOException {
        FileSystemPath cachePath = new FileSystemPath(FileSystem.getLocal(new Configuration()), new Path(timestampPath));
        String timestampName = getTimestampName(timestampPath);
        
        // @formatter:off
        Collection<String> lockJobIds =
                Arrays.stream(jobIds)
                        .map(jobId -> getLockPath(timestampName, jobId))
                        .collect(Collectors.toList());
        // @formatter:on
        return new JobCacheStatus(cachePath, new LockCacheStatus(locked, lockJobIds));
    }
    
    private String getLockPath(String cache, String jobId) {
        return (jobId.isEmpty()) ? cache : cache + File.separator + jobId;
    }
    
    private String getTimestampName(String timestampPath) {
        return timestampPath.substring(timestampPath.lastIndexOf(File.separator) + 1);
    }
}

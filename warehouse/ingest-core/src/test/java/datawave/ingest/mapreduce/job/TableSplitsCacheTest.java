package datawave.ingest.mapreduce.job;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class TableSplitsCacheTest {
    
    public static final String PASSWORD = "passw0rd";
    public static final String HOST_NAME = "localhost";
    public static final String USER_NAME = "staff";
    protected static final Logger logger = Logger.getLogger(TableSplitsCacheTest.class);
    protected static Level testDriverLevel;
    protected static Level uutLevel;
    protected static Level zooCacheLevel;
    protected static Level zooKeeperLevel;
    
    protected static long FILE_STATUS_LENGTH = 1000l;
    protected static boolean FILE_STATUS_IS_DIRECTORY = true;
    protected static int FILE_STATUS_REPLICATION = 3;
    protected static long FILE_STATUS_BLOCK_SIZE = 1024l;
    protected static long FILE_STATUS_MODIFICATION_TIME;
    protected static long FILE_STATUS_ACCESS_TIME;
    protected static String FILE_STATUS_USER;
    protected static String FILE_STATUS_GROUP;
    
    public static class WrappedLocalFileSystem extends RawLocalFileSystem {
        
        @Override
        public FileStatus getFileStatus(Path f) throws IOException {
            
            String path = f.toString();
            if (new File(path).exists()) {
                return new FileStatus(TableSplitsCacheTest.FILE_STATUS_LENGTH, TableSplitsCacheTest.FILE_STATUS_IS_DIRECTORY,
                                TableSplitsCacheTest.FILE_STATUS_REPLICATION, TableSplitsCacheTest.FILE_STATUS_BLOCK_SIZE,
                                TableSplitsCacheTest.FILE_STATUS_MODIFICATION_TIME, TableSplitsCacheTest.FILE_STATUS_ACCESS_TIME, null,
                                TableSplitsCacheTest.FILE_STATUS_USER, TableSplitsCacheTest.FILE_STATUS_GROUP, f);
            } else {
                return null;
            }
            
        }
        
        @Override
        public void setConf(Configuration conf) {
            
            TableSplitsCacheTest.logger.debug("TableSplitsCacheTest.WrappedDistributedFileSystem.setConfig called....");
            
            super.setConf(conf);
        }
        
        @Override
        protected int getDefaultPort() {
            return 8080;
        }
        
        @Override
        public URI getUri() {
            
            return URI.create("file:///localhost");
            
        }
        
        @Override
        public void initialize(URI uri, Configuration conf) throws IOException {
            
            TableSplitsCacheTest.logger.debug("TableSplitsCacheTest.WrappedDistributedFileSystem.initialize called....");
            super.initialize(uri, conf);
        }
        
        @Override
        public void close() throws IOException {
            
            TableSplitsCacheTest.logger.debug("TableSplitsCacheTest.WrappedDistributedFileSystem.close called....");
            
            try {
                super.close();
            } catch (Throwable t) {
                TableSplitsCacheTest.logger.debug(String.format(
                                "DistributedFileSystem handled base class excpetion: %s with messge %s (caused by the missing DfsClient instance...)", t
                                                .getClass().getName(), t.getMessage()));
            }
        }
    }
    
    protected void createMockFileSystem() throws Exception {
        
        FileSystem fs = new TableSplitsCacheTest.WrappedLocalFileSystem();
        
        mockConfiguration.put(FileSystem.FS_DEFAULT_NAME_KEY, "file:///localhost");
        
        // Lifted from DfsMonitorTest
        mockConfiguration.put("fs.file.impl", TableSplitsCacheTest.WrappedLocalFileSystem.class.getName());
        mockConfiguration.put("fs.automatic.close", "false");
        mockConfiguration.put(MRJobConfig.CACHE_FILES, ".");
        
        Configuration conf = createMockConfiguration();
        fs.setConf(conf);
        fs.initialize(URI.create("file:///localhost"), conf);
        
        Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", FileSystem.getDefaultUri(conf), conf, fs);
        
    }
    
    @Before
    public void setup() throws Exception {
        
        mockConfiguration.clear();
        mockConfiguration.put(TableSplitsCache.REFRESH_SPLITS, "false");
        testDriverLevel = logger.getLevel();
        logger.setLevel(Level.ALL);
        
        Logger uutLog = Logger.getLogger(TableSplitsCache.class);
        uutLevel = uutLog.getLevel();
        uutLog.setLevel(Level.ALL);
        
        Logger zcLog = Logger.getLogger(ZooCache.class);
        zooCacheLevel = zcLog.getLevel();
        zcLog.setLevel(Level.ALL);
        
        Logger zkLog = Logger.getLogger(ZooKeeper.class);
        zooKeeperLevel = zkLog.getLevel();
        zkLog.setLevel(Level.ALL);
        
        createMockFileSystem();
        
        TableSplitsCacheTest.FILE_STATUS_MODIFICATION_TIME = TableSplitsCacheTest.FILE_STATUS_ACCESS_TIME = 0l;
        
        TableSplitsCacheTest.FILE_STATUS_USER = TableSplitsCacheTest.USER_NAME;
        TableSplitsCacheTest.FILE_STATUS_GROUP = TableSplitsCacheTest.USER_NAME;
    }
    
    public void setSplitsCacheDir() {
        URL url = TableSplitsCacheTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");
        Assert.assertNotNull("TableSplitsCacheTest#setup failed to load test cache directory.", url);
        mockConfiguration.put(TableSplitsCache.SPLITS_CACHE_DIR, url.getPath().substring(0, url.getPath().lastIndexOf(Path.SEPARATOR)));
    }
    
    public void setSplitsCacheDir(String splitsCacheDir) {
        mockConfiguration.put(TableSplitsCache.SPLITS_CACHE_DIR, splitsCacheDir);
    }
    
    @After
    public void teardown() {
        
        logger.setLevel(testDriverLevel);
        Logger.getLogger(TableSplitsCache.class).setLevel(uutLevel);
        Logger.getLogger(ZooCache.class).setLevel(zooCacheLevel);
        Logger.getLogger(ZooKeeper.class).setLevel(zooKeeperLevel);
        
    }
    
    @AfterClass
    public static void teardownClass() throws Exception {
        
        Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", URI.create("file:///localhost"), null, null);
    }
    
    protected Map<String,String> mockConfiguration = new HashMap<>();
    
    protected void setupConfiguration() {
        
        mockConfiguration.put(AccumuloHelper.USERNAME, USER_NAME);
        mockConfiguration.put(AccumuloHelper.INSTANCE_NAME, HOST_NAME);
        mockConfiguration.put(AccumuloHelper.PASSWORD, Base64.encodeBase64String(PASSWORD.getBytes()));
        mockConfiguration.put(AccumuloHelper.ZOOKEEPERS, HOST_NAME);
        mockConfiguration.put(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        
        TableSplitsCacheTest.FILE_STATUS_MODIFICATION_TIME = Long.MAX_VALUE;
        TableSplitsCacheTest.FILE_STATUS_ACCESS_TIME = 0l;
        
    }
    
    protected JobConf createMockJobConf() {
        JobConf mocked = PowerMock.createMock(JobConf.class);
        
        mocked.get(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String results = (String) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                results = mockConfiguration.get(key);
            }
            
            return results;
        }).anyTimes();
        
        mocked.getLong(EasyMock.anyObject(String.class), EasyMock.anyLong());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            long results = (Long) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Long.parseLong(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.get(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            
            return mockConfiguration.get(key);
        }).anyTimes();
        
        mocked.getBoolean(EasyMock.anyObject(String.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().andAnswer(() -> {
            String key = (String) EasyMock.getCurrentArguments()[0];
            boolean results = (Boolean) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Boolean.parseBoolean(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.getInt(EasyMock.anyObject(String.class), EasyMock.anyInt());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            int results = (Integer) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Integer.parseInt(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.set(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            mockConfiguration.put(key, value);
            
            return null;
        }).anyTimes();
        
        mocked.getStrings(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            String key = (String) EasyMock.getCurrentArguments()[0];
            String[] results = null;
            
            if (mockConfiguration.containsKey(key)) {
                results = StringUtils.getStrings(mockConfiguration.get(key));
            }
            
            return results;
        }).anyTimes();
        
        PowerMock.replay(mocked);
        
        return mocked;
    }
    
    protected Configuration createMockConfiguration() {
        
        Configuration mocked = PowerMock.createMock(Configuration.class);
        
        mocked.get(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String results = (String) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                results = mockConfiguration.get(key);
            }
            
            return results;
        }).anyTimes();
        
        mocked.getLong(EasyMock.anyObject(String.class), EasyMock.anyLong());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            long results = (Long) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Long.parseLong(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.get(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            
            return mockConfiguration.get(key);
        }).anyTimes();
        
        mocked.getBoolean(EasyMock.anyObject(String.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().andAnswer(() -> {
            String key = (String) EasyMock.getCurrentArguments()[0];
            boolean results = (Boolean) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Boolean.parseBoolean(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.getInt(EasyMock.anyObject(String.class), EasyMock.anyInt());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            int results = (Integer) EasyMock.getCurrentArguments()[1];
            
            if (mockConfiguration.containsKey(key)) {
                
                try {
                    
                    results = Integer.parseInt(mockConfiguration.get(key));
                    
                } catch (Throwable t) {
                    
                    logger.debug(String.format("MockConfiguration#getLong threw exception: %s", t.getClass().getName()));
                }
            }
            
            return results;
        }).anyTimes();
        
        mocked.set(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            mockConfiguration.put(key, value);
            
            return null;
        }).anyTimes();
        
        mocked.getStrings(EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            String key = (String) EasyMock.getCurrentArguments()[0];
            String[] results = null;
            
            if (mockConfiguration.containsKey(key)) {
                
                results = StringUtils.getStrings(mockConfiguration.get(key));
            }
            
            return results;
        }).anyTimes();
        
        PowerMock.replay(mocked);
        
        return mocked;
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithNullArgument() {
        logger.info("testCtorWithNullArgument called...");
        setSplitsCacheDir();
        
        try {
            
            new TableSplitsCache(null);
            
            Assert.fail();
            
        } finally {
            
            logger.info("testCtorWithNullArgument completed.");
        }
    }
    
    @Test
    public void testCtorWithValidArgument() throws IOException {
        logger.info("testCtorWithValidArgument called...");
        setSplitsCacheDir();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
        } finally {
            logger.info("testCtorWithValidArgument completed.");
        }
    }
    
    @Test(expected = IOException.class)
    public void testGetSplitsNoFile() throws IOException {
        logger.info("testGetSplitsNoFile called...");
        setupConfiguration();
        
        try {
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            Assert.assertNull("TableSplitsCache#getSplits() created a map of tables and their splits when no file should exist", uut.getSplits());
        } finally {
            
            logger.info("testGetSplitsNoFile completed.");
        }
    }
    
    @Test(expected = IOException.class)
    public void testUpdateNoFile() throws IOException {
        logger.info("testUpdateNoFile called...");
        setupConfiguration();
        setSplitsCacheDir(String.format("/random/dir%s/must/not/exist", (int) (Math.random() * 100) + 1));
        try {
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            uut.update();
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            Assert.assertNull("TableSplitsCache should have no splits", uut.getSplits());
        } finally {
            
            logger.info("testUpdateNoFile completed.");
        }
        
    }
    
    @Test
    public void testGetFileStatus() {
        logger.info("testGetFileStatus called...");
        setSplitsCacheDir();
        setupConfiguration();
        try {
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            FileStatus fileStatus = uut.getFileStatus();
            Assert.assertNotNull("FileStatus", fileStatus);
        } finally {
            
            logger.info("testGetFileStatus completed.");
        }
        
    }
    
    @Test
    public void testGetSplitsNoArgument() throws IOException {
        logger.info("testGetSplitsNoArgument called...");
        setSplitsCacheDir();
        setupConfiguration();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
            Map<String,List<Text>> resultsSet = uut.getSplits();
            
            Assert.assertNotNull("TableSplitsCache#getSplits() failed created a map of tables and their splits", resultsSet);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated map of tables and their splits", resultsSet.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated map of tables and their splits", 3, resultsSet.size());
            
            List<Text> listings = resultsSet.get("shard");
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", listings);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated the list of splits", listings.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated the list of splits", 5, listings.size());
            
            listings = resultsSet.get("shard1");
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", listings);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated the list of splits", listings.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated the list of splits", 1, listings.size());
            
        } finally {
            
            logger.info("testGetSplitsNoArgument completed.");
        }
    }
    
    @Test
    public void testGetSplitsWithArgumentThatMatches() throws IOException {
        logger.info("testGetSplitsWithArgumentThatMatches called...");
        setSplitsCacheDir();
        setupConfiguration();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
            List<Text> resultsSet = uut.getSplits("shard");
            
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", resultsSet);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated the list of splits", resultsSet.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated the list of splits", 5, resultsSet.size());
        } finally {
            
            logger.info("testGetSplitsWithArgumentThatMatches completed.");
        }
    }
    
    @Test
    public void testGetSplitsWithArgumentThatDoesNotMatch() throws IOException {
        logger.info("testGetSplitsWithArgumentThatDoesNotMatch called...");
        setSplitsCacheDir();
        setupConfiguration();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
            List<Text> resultsSet = uut.getSplits("bad-table");
            
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", resultsSet);
            Assert.assertTrue("TableSplitsCache#getSplits() incorrectly populated the list of splits", resultsSet.isEmpty());
            
        } finally {
            
            logger.info("testGetSplitsWithArgumentThatDoesNotMatch completed.");
        }
    }
    
    @Test
    public void testGetSplitsTrimByPassed() throws IOException {
        logger.info("testGetSplitsTrimByPassed called...");
        setSplitsCacheDir();
        setupConfiguration();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
            List<Text> resultsSet = uut.getSplits("shard", Integer.MAX_VALUE);
            
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", resultsSet);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated the list of splits", resultsSet.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated the list of splits", 5, resultsSet.size());
            
        } finally {
            
            logger.info("testGetSplitsTrimByPassed completed.");
        }
    }
    
    @Test
    public void testGetSplitsTrimmed() throws IOException {
        logger.info("testGetSplitsTrimmed called...");
        setSplitsCacheDir();
        setupConfiguration();
        
        try {
            
            TableSplitsCache uut = new TableSplitsCache(createMockJobConf());
            
            Assert.assertNotNull("TableSplitsCache constructor failed to construct an instance.", uut);
            
            List<Text> resultsSet = uut.getSplits("shard", 2);
            
            Assert.assertNotNull("TableSplitsCache#getSplits() failed to a list of splits", resultsSet);
            Assert.assertFalse("TableSplitsCache#getSplits() incorrectly populated the list of splits", resultsSet.isEmpty());
            Assert.assertEquals("TableSplitsCache#getSplits() incorrectly populated the list of splits", 2, resultsSet.size());
            
        } finally {
            
            logger.info("testGetSplitsTrimmed completed.");
        }
    }
    
    @Test
    public void testTrimTableSplits() {
        setSplitsCacheDir();
        int reducers = 487;
        int numsplits = 195365;
        // generate the initial splits
        List<Text> splits = new ArrayList<>(numsplits);
        for (int i = 0; i < numsplits; i++) {
            splits.add(new Text(Integer.toString(i)));
        }
        List<Text> newSplits = TableSplitsCache.trimSplits(splits, reducers - 1);
        Assert.assertEquals("split count", (reducers - 1), newSplits.size());
        Assert.assertEquals("first split", 401, Integer.parseInt(newSplits.get(0).toString()));
        Assert.assertEquals("num splits - last split", 402, numsplits - Integer.parseInt(newSplits.get(newSplits.size() - 1).toString()));
        int lastsplit = 0;
        for (int i = 0; i < newSplits.size(); i++) {
            int split = Integer.parseInt(newSplits.get(i).toString());
            Assert.assertTrue("split size", (split - lastsplit) >= 401);
            Assert.assertTrue("split size", (split - lastsplit) <= 402);
            lastsplit = split;
        }
    }
    
}

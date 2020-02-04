package datawave.ingest.mapreduce.job;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.common.test.logging.CommonTestAppender;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class MultiRFileOutputFormatterTest {
    
    private static final String JOB_ID = "job_201109071404_1";
    private List<String> filenames = new ArrayList<>();
    protected static final Logger logger = Logger.getLogger(MultiRFileOutputFormatterTest.class);
    protected static Map<String,String> mockedConfiguration = new HashMap<>();
    
    protected Level testDriverLevel;
    protected Level uutLevel;
    protected CommonTestAppender uutAppender;
    private MultiRFileOutputFormatter formatter;
    private Configuration conf;
    
    protected List<String> retrieveUUTLogs() throws IOException {
        
        return uutAppender.retrieveLogsEntries();
    }
    
    protected boolean checkProcessOutput(List<String> output, String message) {
        
        boolean results = false;
        
        for (String msg : output) {
            
            results = msg.contains(message);
            
            if (results) {
                
                break;
            }
        }
        
        return results;
    }
    
    protected Configuration createMockConfiguration() {
        
        MultiRFileOutputFormatterTest.mockedConfiguration.clear();
        
        Configuration mocked = PowerMock.createMock(Configuration.class);
        
        MultiRFileOutputFormatterTest.logger.info(String.format("createMockConfiguration: %d", mocked.hashCode()));
        
        mocked.set(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            MultiRFileOutputFormatterTest.mockedConfiguration.put(key, value);
            
            return null;
        }).anyTimes();
        
        mocked.setStrings(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            if (2 <= EasyMock.getCurrentArguments().length) {
                
                String key = (String) EasyMock.getCurrentArguments()[0];
                String[] values = new String[EasyMock.getCurrentArguments().length - 1];
                
                for (int index = 1; index <= values.length; index++) {
                    
                    values[index - 1] = (String) EasyMock.getCurrentArguments()[index];
                }
                
                MultiRFileOutputFormatterTest.mockedConfiguration.put(key, StringUtils.arrayToString(values));
            }
            return null;
        }).anyTimes();
        
        mocked.get(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            if (MultiRFileOutputFormatterTest.mockedConfiguration.containsKey(key)) {
                
                value = MultiRFileOutputFormatterTest.mockedConfiguration.get(key);
            }
            
            return value;
        }).anyTimes();
        
        PowerMock.replay(mocked);
        
        return mocked;
    }
    
    @Before
    public void setup() throws Exception {
        
        testDriverLevel = MultiRFileOutputFormatterTest.logger.getLevel();
        MultiRFileOutputFormatterTest.logger.setLevel(Level.ALL);
        
        Logger uutLogger = Logger.getLogger(ShardedTableMapFile.class);
        uutAppender = new CommonTestAppender();
        
        uutLevel = uutLogger.getLevel();
        uutLogger.setLevel(Level.ALL);
        uutLogger.addAppender(uutAppender);
        
        MultiRFileOutputFormatterTest.mockedConfiguration.clear();
    }
    
    @After
    public void teardown() {
        
        MultiRFileOutputFormatterTest.logger.setLevel(testDriverLevel);
        MultiRFileOutputFormatterTest.logger.removeAppender(uutAppender);
        Logger.getLogger(MultiRFileOutputFormatter.class).setLevel(uutLevel);
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSetCompressionTypeWithBadType() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType called...");
        
        try {
            
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), "bad-compressor");
            
            Assert.fail("MultiRFileOutputFormatter#setCompressionType failed to throw expected exception.");
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType completed.");
            
        }
    }
    
    @Test
    public void testSetCompressionType() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType called...");
        
        try {
            
            String compressionKey = String.format("%s.compression", MultiRFileOutputFormatter.class.getName());
            
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), null);
            
            Assert.assertFalse("MultiRFileOutputFormatter.setCompressionType set compression type when it should not have.",
                            MultiRFileOutputFormatterTest.mockedConfiguration.containsKey(compressionKey));
            
            String expected = "snappy";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));
            
            expected = "lzo";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));
            
            expected = "gz";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));
            
            expected = "none";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType completed.");
            
        }
    }
    
    @Test
    public void testGetCompressionTypeWithoutSetting() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType called...");
        
        try {
            
            String expected = "gz";
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(createMockConfiguration()));
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetCompressionTypeWithBadType completed.");
            
        }
    }
    
    @Test
    public void testGetCompressionType() {
        
        MultiRFileOutputFormatterTest.logger.info("testGetCompressionType called...");
        
        try {
            
            Configuration conf = createMockConfiguration();
            String expected = "snappy";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));
            
            expected = "lzo";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));
            
            expected = "gz";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));
            
            expected = "none";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);
            
            Assert.assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testGetCompressionType completed.");
            
        }
    }
    
    @Test
    public void testSetFileType() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetFileType called...");
        
        try {
            
            String typeKey = String.format("%s.file_type", MultiRFileOutputFormatter.class.getSimpleName());
            Configuration conf = createMockConfiguration();
            
            Assert.assertFalse("mockedConfiguration incorrectly contained a File Type property",
                            MultiRFileOutputFormatterTest.mockedConfiguration.containsKey(typeKey));
            
            MultiRFileOutputFormatter.setFileType(conf, null);
            
            Assert.assertFalse("MultiRFileOutputFormatter#setFileType incorrectly added a File Type property",
                            MultiRFileOutputFormatterTest.mockedConfiguration.containsKey(typeKey));
            
            MultiRFileOutputFormatter.setFileType(conf, "fileType");
            
            Assert.assertTrue("MultiRFileOutputFormatter#setFileType failed to add a File Type property",
                            MultiRFileOutputFormatterTest.mockedConfiguration.containsKey(typeKey));
            Assert.assertEquals("MultiRFileOutputFormatter#setFileType failed to retain the expected File Type property value", "fileType",
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(typeKey));
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetFileType completed.");
            
        }
    }
    
    @Test
    public void testSetAccumuloConfiguration() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetFileType called...");
        
        try {
            
            String instance = "instance";
            String username = "user";
            byte[] password = "passw0rd".getBytes();
            String zookeepers = "zookeepers";
            Configuration conf = new Configuration();
            
            AccumuloHelper.setInstanceName(conf, instance);
            AccumuloHelper.setUsername(conf, username);
            AccumuloHelper.setPassword(conf, password);
            AccumuloHelper.setZooKeepers(conf, zookeepers);
            
            MultiRFileOutputFormatter.setAccumuloConfiguration(conf);
            createMockConfiguration();
            
            String instanceKey = String.format("%s.instance.name", MultiRFileOutputFormatter.class.getName());
            String usernameKey = String.format("%s.username", MultiRFileOutputFormatter.class.getName());
            String passwordKey = String.format("%s.password", MultiRFileOutputFormatter.class.getName());
            String zooKeeperKey = String.format("%s.zookeepers", MultiRFileOutputFormatter.class.getName());
            
            Assert.assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected instance value.", instance,
                            conf.get(instanceKey));
            Assert.assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected username value.", username,
                            conf.get(usernameKey));
            Assert.assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected password value.",
                            new String(Base64.encodeBase64(password)), conf.get(passwordKey));
            Assert.assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected zookeepers value.", zookeepers,
                            conf.get(zooKeeperKey));
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetFileType completed.");
            
        }
    }
    
    @Test
    public void testCtor() {
        
        MultiRFileOutputFormatterTest.logger.info("testSetFileType called...");
        
        try {
            
            MultiRFileOutputFormatter uut = new MultiRFileOutputFormatter();
            
            Assert.assertNotNull("MultiRFileOutputFormatter constructor failed to create an instance.", uut);
            
        } finally {
            
            MultiRFileOutputFormatterTest.logger.info("testSetFileType completed.");
            
        }
    }
    
    private MultiRFileOutputFormatter createFormatter() {
        this.filenames.clear();
        return new MultiRFileOutputFormatter() {
            @Override
            protected Set<String> getTableList() {
                Set<String> tables = new HashSet<>();
                tables.add(TableName.SHARD);
                tables.add(TableName.SHARD_INDEX);
                return tables;
            }
            
            @Override
            protected void setTableIdsAndConfigs() {
                tableConfigs = new HashMap<>();
                tableConfigs.put(TableName.SHARD, null);
                tableConfigs.put(TableName.SHARD_INDEX, null);
                tableIds = new HashMap<>();
                tableIds.put(TableName.SHARD, "1");
                tableIds.put(TableName.SHARD_INDEX, "2");
            }
            
            @Override
            protected int getSeqFileBlockSize() {
                return 1;
            }
            
            @Override
            protected Map<Text,String> getShardLocations(String tableName) throws IOException {
                Map<Text,String> locations = new HashMap<>();
                locations.put(new Text("20100101_1"), "server1");
                locations.put(new Text("20100101_2"), "server2");
                return locations;
            }
            
            @Override
            protected SizeTrackingWriter openWriter(String filename, AccumuloConfiguration tableConf) {
                filenames.add(filename);
                return new SizeTrackingWriter(new FileSKVWriter() {
                    
                    @Override
                    public boolean supportsLocalityGroups() {
                        return false;
                    }
                    
                    @Override
                    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {}
                    
                    @Override
                    public void startDefaultLocalityGroup() throws IOException {}
                    
                    @Override
                    public DataOutputStream createMetaStore(String name) throws IOException {
                        return null;
                    }
                    
                    @Override
                    public void close() throws IOException {}
                    
                    @Override
                    public long getLength() throws IOException {
                        return 0;
                    }
                    
                    @Override
                    public void append(Key key, Value value) throws IOException {}
                });
                
            }
        };
        
    }
    
    @Before
    public void before() {
        formatter = createFormatter();
        conf = new Configuration();
        conf.set("mapred.output.dir", "/tmp");
        conf.set(MultiRFileOutputFormatter.CONFIGURED_TABLE_NAMES, TableName.SHARD + ',' + TableName.SHARD_INDEX);
        Map<String,Path> shardedTableMapFiles = new HashMap<>();
        shardedTableMapFiles.put("shard", new Path("/tmp/shard"));
        ShardedTableMapFile.addToConf(conf, shardedTableMapFiles);
    }
    
    @Test
    public void testTableSeparation() throws IOException, InterruptedException {
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        writeShardPairs(writer, 2);
        assertNumFileNames(2);
        assertFileNameForShardIndex(0);
        expectShardFiles(1);
    }
    
    @Test
    public void testTableSeparationWithFilePerShardLoc() throws IOException, InterruptedException {
        MultiRFileOutputFormatter.setGenerateMapFilePerShardLocation(conf, true);
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        writeShardPairs(writer, 2);
        assertNumFileNames(3);
        assertFileNameForShardIndex(0);
        assertFileNameForShard(1, "server1", 1);
        assertFileNameForShard(2, "server2", 1);
    }
    
    @Test
    public void testRFileEntrySizeLimit() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, URISyntaxException {
        MultiRFileOutputFormatter.setRFileLimits(conf, 1, 0);
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        writeShardPairs(writer, 2);
        assertNumFileNames(5);
        assertFileNameForShardIndex(0);
        expectShardFiles(4);
    }
    
    private void expectShardFiles(int num) {
        for (int i = 1; i <= num; i++) {
            assertFileNameForShard(i, "shards", i);
        }
    }
    
    @Test
    public void testRFileFileSizeLimitWithFilePerShardLoc() throws IOException, InterruptedException {
        MultiRFileOutputFormatter.setGenerateMapFilePerShardLocation(conf, true);
        // each key we write is 16 characters total, so a limit of 32 should allow two keys per file
        MultiRFileOutputFormatter.setRFileLimits(conf, 0, 32);
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        writeShardPairs(writer, 2);
        assertNumFileNames(3);
        assertFileNameForShardIndex(0);
        assertFileNameForShard(1, "server1", 1);
        assertFileNameForShard(2, "server2", 1);
    }
    
    @Test
    public void testRFileFileSizeLimit() throws IOException, InterruptedException {
        // each key we write is 16 characters total, so a limit of 32 should allow two keys per file
        MultiRFileOutputFormatter.setRFileLimits(conf, 0, 32);
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        writeShardPairs(writer, 3);
        assertNumFileNames(4);
        assertFileNameForShardIndex(0);
        expectShardFiles(3);
    }
    
    @Test
    public void testRFileEntrySizeLimitWithFilePerShardLoc() throws IOException, InterruptedException {
        MultiRFileOutputFormatter.setRFileLimits(conf, 1, 0);
        MultiRFileOutputFormatter.setGenerateMapFilePerShardLocation(conf, true);
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, conf);
        assertFileNameForShardIndex(0);
        writeShardPairs(writer, 2);
        assertNumFileNames(5);
        assertFileNameForShardIndex(0);
        assertFileNameForShard(1, "server1", 1);
        assertFileNameForShard(2, "server2", 1);
        assertFileNameForShard(3, "server1", 2);
        assertFileNameForShard(4, "server2", 2);
    }
    
    private void writeShardPairs(RecordWriter<BulkIngestKey,Value> writer, int numOfPairs) throws IOException, InterruptedException {
        for (int i = 0; i < numOfPairs; i++) {
            writeShardEntry(writer, 1);
            writeShardEntry(writer, 2);
        }
    }
    
    private void assertFileNameForShardIndex(int index) {
        Assert.assertTrue(filenames.get(index).endsWith("/shardIndex/shardIndex-m-00001_1.rf"));
    }
    
    private RecordWriter<BulkIngestKey,Value> createWriter(MultiRFileOutputFormatter formatter, Configuration conf) throws IOException, InterruptedException {
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1));
        return formatter.getRecordWriter(context);
    }
    
    private void assertFileNameForShard(int index, String prefix, int shardId) {
        Assert.assertTrue(filenames.get(index).endsWith("/shard/" + prefix + "-m-00001_" + shardId + ".rf"));
    }
    
    private void writeShardEntry(RecordWriter<BulkIngestKey,Value> writer, int shardId) throws IOException, InterruptedException {
        writer.write(new BulkIngestKey(new Text(TableName.SHARD), new Key("20100101_" + shardId, "bla", "bla")), new Value(new byte[0]));
    }
    
    private void assertNumFileNames(int expectedNumFiles) {
        Assert.assertEquals(filenames.toString(), expectedNumFiles, filenames.size());
    }
}

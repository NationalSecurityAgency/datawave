package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.BULK_IMPORT_MODE_CONFIG;
import static datawave.ingest.mapreduce.job.MultiRFileOutputFormatter.getKeyExtent;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
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

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.ImportMode;
import datawave.util.TableName;

public class MultiRFileOutputFormatterTest {

    private static final String JOB_ID = "job_201109071404_1";
    private List<String> filenames = new ArrayList<>();
    protected static final Logger logger = Logger.getLogger(MultiRFileOutputFormatterTest.class);
    protected static Map<String,String> mockedConfiguration = new HashMap<>();

    protected Level testDriverLevel;
    protected Level uutLevel;
    private MultiRFileOutputFormatter formatter;
    private Configuration conf;

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

        MultiRFileOutputFormatterTest.mockedConfiguration.clear();
    }

    @After
    public void teardown() {

        MultiRFileOutputFormatterTest.logger.setLevel(testDriverLevel);
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

    private Map<String,Set<LoadPlan>> tableLoadPlans = new HashMap<>();

    private ArrayList<Text> getSplits(String table) {
        var arr = new ArrayList<Text>();
        arr.add(new Text("20170601_0")); // 0
        arr.add(new Text("20170601_1")); // 1
        arr.add(new Text("20170601_2")); // 2
        arr.add(new Text("20170601_3")); // 3
        arr.add(new Text("20170601_4")); // 4
        arr.add(new Text("20170601_5")); // 5
        arr.add(new Text("20170601_6")); // 6
        arr.add(new Text("20170601_7")); // 7
        arr.add(new Text("20170601_8")); // 8
        arr.add(new Text("20170601_9")); // 9
        arr.add(new Text("20170602_0")); // 10
        arr.add(new Text("20170602_1")); // 11
        arr.add(new Text("20170602_2")); // 12
        arr.add(new Text("20170602_3")); // 13
        arr.add(new Text("20170602_4")); // 14
        arr.add(new Text("20170602_5")); // 15
        arr.add(new Text("20170602_6")); // 16
        arr.add(new Text("20170602_7")); // 17
        arr.add(new Text("20170602_8")); // 18

        arr.add(new Text("20170602_9")); // 19
        arr.add(new Text("20170602_9a")); // 20
        arr.add(new Text("20170602_9b")); // 21
        arr.add(new Text("20170602_9c")); // 22

        arr.add(new Text("20170603_0")); // 23
        arr.add(new Text("20170603_0a")); // 24
        arr.add(new Text("20170603_0b")); // 25
        arr.add(new Text("20170603_0c")); // 26

        arr.add(new Text("20170603_1")); // 27
        arr.add(new Text("20170603_2")); // 28
        arr.add(new Text("20170603_3")); // 29
        arr.add(new Text("20170603_4")); // 30
        arr.add(new Text("20170603_5")); // 31
        arr.add(new Text("20170603_6")); // 32
        arr.add(new Text("20170603_7")); // 34
        arr.add(new Text("20170603_8")); // 35
        arr.add(new Text("20170603_9")); // 36
        return arr;
    }

    @Test
    public void testPlanning() {
        SortedSet<Text> rows = new TreeSet<>();
        rows.add(new Text("20160602_0"));
        rows.add(new Text("20170601_0"));
        rows.add(new Text("20170601_1"));
        rows.add(new Text("20170602_1"));
        rows.add(new Text("20170602_0a1"));
        rows.add(new Text("20170602_0a11"));
        rows.add(new Text("20170602_0a111"));
        rows.add(new Text("20170602_0b1"));
        rows.add(new Text("20170602_0c1"));
        rows.add(new Text("20170603_0"));
        rows.add(new Text("20170603_0a11"));
        rows.add(new Text("20170603_0a12"));
        rows.add(new Text("20170603_0b"));
        rows.add(new Text("20170603_0c"));
        rows.add(new Text("20170603_0d"));
        rows.add(new Text("20170601_9"));
        rows.add(new Text("20200601_9"));
        addLoadPlanForFile(rows, "shard", new Path("/path/to/file"));
    }

    private void addLoadPlanForFile(SortedSet<Text> rows, String table, Path filepath) {

        Map<String,Set<LoadPlan>> tableLoadPlans = new HashMap<>();
        if (!tableLoadPlans.containsKey(table)) {
            tableLoadPlans.put(table, new HashSet<>());
        }
        if (rows != null && rows.size() > 0) {
            /*
             * @param endRow the last row in this tablet, or null if this is the last tablet in this table
             *
             * @param prevEndRow the last row in the immediately preceding tablet for the table, or null if this represents the first tablet in this table
             */
            List<Text> tableSplits = getSplits(table);
            rows.stream().map(row -> getKeyExtent(row, tableSplits)).collect(Collectors.toCollection(HashSet::new));
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

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));

            expected = "lzo";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));

            expected = "gz";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatterTest.mockedConfiguration.get(compressionKey));

            expected = "none";
            MultiRFileOutputFormatter.setCompressionType(createMockConfiguration(), expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
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

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
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

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));

            expected = "lzo";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));

            expected = "gz";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
                            MultiRFileOutputFormatter.getCompressionType(conf));

            expected = "none";
            MultiRFileOutputFormatter.setCompressionType(conf, expected);

            assertEquals("MultiRFileOutputFormatter.setCompressionType failed to set compression type", expected,
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
            assertEquals("MultiRFileOutputFormatter#setFileType failed to retain the expected File Type property value", "fileType",
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

            assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected instance value.", instance, conf.get(instanceKey));
            assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected username value.", username, conf.get(usernameKey));
            assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected password value.",
                            new String(Base64.encodeBase64(password)), conf.get(passwordKey));
            assertEquals("MultiRFileOutputFormatter#setAccumuloConfiguration failed to retain the expected zookeepers value.", zookeepers,
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
                tableIds = new HashSet<>(Arrays.asList(TableName.SHARD, TableName.SHARD_INDEX));

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
                }, false);
            }
        };

    }

    @Before
    public void before() {
        formatter = createFormatter();
        conf = new Configuration();
        conf.set("mapred.output.dir", "/tmp");
        conf.set(SplitsFile.CONFIGURED_SHARDED_TABLE_NAMES, TableName.SHARD);
        conf.setEnum(BULK_IMPORT_MODE_CONFIG, ImportMode.V2_LOAD_PLANNING);
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
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1));
        RecordWriter<BulkIngestKey,Value> writer = createWriter(formatter, context);
        writeShardPairs(writer, 3);
        // writer.close(context);
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

    private RecordWriter<BulkIngestKey,Value> createWriter(MultiRFileOutputFormatter formatter, TaskAttemptContext context)
                    throws IOException, InterruptedException {
        return formatter.getRecordWriter(context);
    }

    private void assertFileNameForShard(int index, String prefix, int shardId) {
        Assert.assertTrue(filenames.get(index).endsWith("/shard/" + prefix + "-m-00001_" + shardId + ".rf"));
    }

    private void writeShardEntry(RecordWriter<BulkIngestKey,Value> writer, int shardId) throws IOException, InterruptedException {
        writer.write(new BulkIngestKey(new Text(TableName.SHARD), new Key("20100101_" + shardId, "bla", "bla")), new Value(new byte[0]));
    }

    private void assertNumFileNames(int expectedNumFiles) {
        assertEquals(filenames.toString(), expectedNumFiles, filenames.size());
    }
}

package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.AccumuloRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import datawave.common.test.logging.TestLogCollector;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.table.hash.UID;

public class CBMutationOutputFormatterTest {

    protected static final String SIMULATION_MODE = "AccumuloOutputFormat.Features.SimulationMode";
    protected static final Logger logger = Logger.getLogger(CBMutationOutputFormatterTest.class);

    protected Level testDriverLevel;

    @Rule
    public TestLogCollector logCollector = new TestLogCollector.Builder().with(CBMutationOutputFormatter.class, Level.ALL)
                    .with(AccumuloOutputFormat.class, Level.ALL).with(AccumuloRecordWriter.class, Level.ALL).build();

    protected boolean processOutputContains(List<String> output, String message) {
        boolean results = false;

        for (String msg : output) {

            results = msg.contains(message);

            if (results) {

                break;
            }
        }

        return results;
    }

    @Before
    public void setup() {
        testDriverLevel = CBMutationOutputFormatterTest.logger.getLevel();
        CBMutationOutputFormatterTest.logger.setLevel(Level.ALL);
        TypeRegistry.reset();
    }

    @After
    public void teardown() {
        CBMutationOutputFormatterTest.logger.setLevel(testDriverLevel);
    }

    @Test
    public void testCTor() {

        CBMutationOutputFormatterTest.logger.info("testCTor called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

        } finally {

            CBMutationOutputFormatterTest.logger.info("testCTor completed.");
        }
    }

    @Test
    public void testGetRecordWriter() throws IOException {

        CBMutationOutputFormatterTest.logger.info("testGetRecordWriter called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            Configuration conf = new Configuration();

            conf.setBoolean(SIMULATION_MODE, true);

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter#getRecordWriter failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter#getRecordWriter failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testGetRecordWriter completed.");
        }
    }

    @Test
    public void testRecordWriterClose() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterClose called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            Configuration conf = new Configuration();

            conf.setBoolean(SIMULATION_MODE, true);

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            TaskAttemptContext context = null;

            rw.close(context);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#close failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#close failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#close failed to create simulation warning message.",
                            processOutputContains(entries, "mutations written: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterClose completed.");
        }
    }

    @Test
    public void testRecordWriterWriteWithoutTableName() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithoutTableName called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            Configuration conf = new Configuration();

            conf.setBoolean(SIMULATION_MODE, true);

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithoutTableName completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithTableNameNoUpdates() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithTableNameNoUpdates called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            Configuration conf = new Configuration();

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithTableNameNoUpdates completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithTableNameWithUpdates() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithTableNameWithUpdates called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            Configuration conf = new Configuration();

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            value.put("colf".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column: colf:colq"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithTableNameWithUpdates completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithUpdatesAndTypes() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesAndTypes called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            URL url = CBMutationOutputFormatterTest.class.getResource("/datawave/ingest/mapreduce/job/IngestJob-test-type.xml");
            Configuration conf = new Configuration();

            conf.addResource(url);

            TypeRegistry.getInstance(conf);

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            value.put("colf".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column: colf:colq"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesAndTypes completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithUpdatesWithColFamilyTyped() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTyped called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            URL url = CBMutationOutputFormatterTest.class.getResource("/datawave/ingest/mapreduce/job/IngestJob-test-type.xml");
            Configuration conf = new Configuration();

            conf.addResource(url);

            TypeRegistry.getInstance(conf);

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            value.put("ingest-test".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column: ingest-test:colq"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTyped completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithUpdatesWithColFamilyTypedWithUID() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithUID called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            URL url = CBMutationOutputFormatterTest.class.getResource("/datawave/ingest/mapreduce/job/IngestJob-test-type.xml");
            Configuration conf = new Configuration();

            conf.addResource(url);

            TypeRegistry.getInstance(conf);

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            UID testUID = UID.builder().newId("hello world".getBytes(), new Date(System.currentTimeMillis()));
            String colf = String.format("ingest-test%c%s", '\0', testUID.toString());
            value.put(colf.getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column:"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithUID completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithUpdatesWithColFamilyTypedWithBadUID() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithBadUID called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            URL url = CBMutationOutputFormatterTest.class.getResource("/datawave/ingest/mapreduce/job/IngestJob-test-type.xml");
            Configuration conf = new Configuration();

            conf.addResource(url);

            TypeRegistry.getInstance(conf);

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            UID testUID = UID.builder().newId("hello world".getBytes(), new Date(System.currentTimeMillis()));
            String colf = String.format("ingest-test%c%s", '\0', testUID.toString().substring(testUID.toString().length() - 10));
            value.put(colf.getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column:"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithBadUID completed.");
        }

    }

    @Test
    public void testRecordWriterWriteWithUpdatesWithColFamilyTypedWithoutUID() throws IOException, InterruptedException {

        CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithoutUID called...");

        try {

            CBMutationOutputFormatter uut = new CBMutationOutputFormatter();

            Assert.assertNotNull("CBMutationOutputFormatter constructor failed to generate an instance.", uut);

            URL url = CBMutationOutputFormatterTest.class.getResource("/datawave/ingest/mapreduce/job/IngestJob-test-type.xml");
            Configuration conf = new Configuration();

            conf.addResource(url);

            TypeRegistry.getInstance(conf);

            conf.setBoolean(SIMULATION_MODE, true);

            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");

            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);

            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);

            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());

            String colf = "ingest-test-tmp";
            value.put(colf.getBytes(), "colq".getBytes(), "hello, world!!".getBytes());

            rw.write(key, value);

            List<String> entries = logCollector.getMessages();

            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Simulating output only. No writes to tables will occur"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create Event Table name message.",
                            processOutputContains(entries, "Event Table Name property for "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to create simulation warning message.",
                            processOutputContains(entries, "Table test-table row key: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column update message.",
                            processOutputContains(entries, "Table test-table column:"));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table column security message.",
                            processOutputContains(entries, "Table test-table security: "));
            Assert.assertTrue("CBMutationOutputFormatter$getRecordWriter#write failed to table value message.",
                            processOutputContains(entries, "Table test-table value: "));

        } finally {

            CBMutationOutputFormatterTest.logger.info("testRecordWriterWriteWithUpdatesWithColFamilyTypedWithoutUID completed.");
        }

    }

    @Test
    public void testGetClientPropertiesLoadsPreConfiguredProperties() {
        CBMutationOutputFormatterTest.logger.info("testGetClientPropertiesLoadsPreConfiguredProperties called...");

        try {
            Configuration conf = new Configuration();

            // Set DataWave connection configuration
            conf.set(AccumuloHelper.INSTANCE_NAME, "test-instance");
            conf.set(AccumuloHelper.ZOOKEEPERS, "localhost:2181");
            conf.set(AccumuloHelper.USERNAME, "testuser");
            conf.set(AccumuloHelper.PASSWORD, "dGVzdHBhc3M="); // base64 encoded "testpass"

            // Set pre-configured client properties (e.g., batch writer settings from shell scripts)
            String batchWriterProps = "batch.writer.memory.max=100000000B\nbatch.writer.threads.max=8";
            conf.set("AccumuloOutputFormat.ClientOpts.ClientProps", batchWriterProps);

            Properties props = CBMutationOutputFormatter.getClientProperties(conf);

            // Verify pre-configured properties are loaded
            Assert.assertEquals("Pre-configured batch.writer.memory.max should be loaded", "100000000B", props.getProperty("batch.writer.memory.max"));
            Assert.assertEquals("Pre-configured batch.writer.threads.max should be loaded", "8", props.getProperty("batch.writer.threads.max"));

            // Verify DataWave connection properties are overlaid
            Assert.assertEquals("Instance name should be set", "test-instance", props.getProperty("instance.name"));
            Assert.assertEquals("Zookeepers should be set", "localhost:2181", props.getProperty("instance.zookeepers"));
            Assert.assertNotNull("Auth type should be set", props.getProperty("auth.type"));

        } finally {
            CBMutationOutputFormatterTest.logger.info("testGetClientPropertiesLoadsPreConfiguredProperties completed.");
        }
    }

    @Test
    public void testGetClientPropertiesWithoutPreConfiguredProperties() {
        CBMutationOutputFormatterTest.logger.info("testGetClientPropertiesWithoutPreConfiguredProperties called...");

        try {
            Configuration conf = new Configuration();

            // Set DataWave connection configuration only
            conf.set(AccumuloHelper.INSTANCE_NAME, "test-instance");
            conf.set(AccumuloHelper.ZOOKEEPERS, "localhost:2181");
            conf.set(AccumuloHelper.USERNAME, "testuser");
            conf.set(AccumuloHelper.PASSWORD, "dGVzdHBhc3M="); // base64 encoded "testpass"

            Properties props = CBMutationOutputFormatter.getClientProperties(conf);

            // Verify DataWave connection properties are set
            Assert.assertEquals("Instance name should be set", "test-instance", props.getProperty("instance.name"));
            Assert.assertEquals("Zookeepers should be set", "localhost:2181", props.getProperty("instance.zookeepers"));
            Assert.assertNotNull("Auth type should be set", props.getProperty("auth.type"));

            // Batch writer properties should not be present
            Assert.assertNull("batch.writer.memory.max should not be set", props.getProperty("batch.writer.memory.max"));

        } finally {
            CBMutationOutputFormatterTest.logger.info("testGetClientPropertiesWithoutPreConfiguredProperties completed.");
        }
    }
}

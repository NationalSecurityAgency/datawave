package datawave.ingest.mapreduce.job;

import datawave.common.test.logging.CommonTestAppender;
import datawave.ingest.data.TypeRegistry;
import datawave.data.hash.UID;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.TokenSource;
import org.apache.accumulo.core.client.mapreduce.lib.util.OutputConfigurator.Features;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
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

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CBMutationOutputFormatterTest {
    
    protected static final Logger logger = Logger.getLogger(CBMutationOutputFormatterTest.class);
    
    protected Level testDriverLevel;
    protected Level uutLevel;
    protected CommonTestAppender uutAppender;
    
    protected static Map<String,String> mockedConfiguration = new HashMap<>();
    
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
    
    protected boolean wasPropertySet(String prefix, String expectedValue) {
        
        boolean results = false;
        
        for (String key : CBMutationOutputFormatterTest.mockedConfiguration.keySet()) {
            
            if (key.startsWith(prefix)) {
                
                String value = CBMutationOutputFormatterTest.mockedConfiguration.get(key);
                
                results = value.equalsIgnoreCase(expectedValue);
                
                if (results) {
                    
                    break;
                }
            }
            
        }
        
        return results;
    }
    
    protected Configuration createMockConfiguration() {
        
        Configuration mocked = PowerMock.createMock(Configuration.class);
        
        mocked.get(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            if (CBMutationOutputFormatterTest.mockedConfiguration.containsKey(key)) {
                
                value = CBMutationOutputFormatterTest.mockedConfiguration.get(key);
            }
            
            return value;
        }).anyTimes();
        
        mocked.set(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = (String) EasyMock.getCurrentArguments()[1];
            
            CBMutationOutputFormatterTest.mockedConfiguration.put(key, value);
            
            return null;
        }).anyTimes();
        
        mocked.getBoolean(EasyMock.anyObject(String.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            boolean value = (Boolean) EasyMock.getCurrentArguments()[1];
            
            if (CBMutationOutputFormatterTest.mockedConfiguration.containsKey(key)) {
                
                value = Boolean.parseBoolean(CBMutationOutputFormatterTest.mockedConfiguration.get(key));
            }
            
            return value;
        }).anyTimes();
        
        mocked.setBoolean(EasyMock.anyObject(String.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().andAnswer(() -> {
            
            String key = (String) EasyMock.getCurrentArguments()[0];
            String value = EasyMock.getCurrentArguments()[1].toString();
            
            CBMutationOutputFormatterTest.mockedConfiguration.put(key, value);
            
            return null;
        }).anyTimes();
        
        PowerMock.replay(mocked);
        
        return mocked;
    }
    
    protected Job createMockJob() {
        
        Job mocked = PowerMock.createMock(Job.class);
        
        mocked.getConfiguration();
        EasyMock.expectLastCall().andAnswer(this::createMockConfiguration).anyTimes();
        
        PowerMock.replay(mocked);
        
        return mocked;
    }
    
    @Before
    public void setup() {
        
        testDriverLevel = CBMutationOutputFormatterTest.logger.getLevel();
        CBMutationOutputFormatterTest.logger.setLevel(Level.ALL);
        
        uutAppender = new CommonTestAppender();
        Logger uutLogger = Logger.getLogger(CBMutationOutputFormatter.class);
        uutLevel = uutLogger.getLevel();
        uutLogger.setLevel(Level.ALL);
        uutLogger.addAppender(uutAppender);
        
        Logger.getLogger(AccumuloOutputFormat.class).addAppender(uutAppender);
        
        TypeRegistry.reset();
    }
    
    @After
    public void teardown() {
        
        Logger.getLogger(CBMutationOutputFormatter.class).setLevel(uutLevel);
        Logger.getLogger(CBMutationOutputFormatter.class).removeAppender(uutAppender);
        Logger.getLogger(AccumuloOutputFormat.class).removeAppender(uutAppender);
        CBMutationOutputFormatterTest.logger.setLevel(testDriverLevel);
    }
    
    @Test
    public void testSetZooKeeperInstance() {
        
        CBMutationOutputFormatterTest.logger.info("testSetZooKeeperInstance called...");
        
        try {
            
            Job job = createMockJob();
            String instanceName = "localhost";
            String zooKeepers = "zookeeper";
            
            CBMutationOutputFormatter.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
            
            Assert.assertTrue("CBMutationOutputFormatter#setZooKeeperInstance failed to set 'Type'",
                            CBMutationOutputFormatterTest.mockedConfiguration.containsValue("ZooKeeperInstance"));
            Assert.assertTrue("CBMutationOutputFormatter#setZooKeeperInstance failed to set 'Type'",
                            this.wasPropertySet(AccumuloOutputFormat.class.getSimpleName(), "ZooKeeperInstance"));
            
        } finally {
            
            CBMutationOutputFormatterTest.logger.info("testSetZooKeeperInstance completed.");
        }
    }
    
    @Test
    public void testSetOutputInfo() throws AccumuloSecurityException {
        
        CBMutationOutputFormatterTest.logger.info("testSetOutputInfo called...");
        
        try {
            
            Job job = createMockJob();
            String user = "user";
            byte[] passwd = "passw0rd".getBytes();
            boolean createTables = false;
            String defaultTable = "default-table";
            
            CBMutationOutputFormatter.setOutputInfo(job, user, passwd, createTables, defaultTable);
            
            PasswordToken pt = new PasswordToken(passwd);
            String ptStr = TokenSource.INLINE.prefix() + PasswordToken.class.getName() + ":"
                            + Base64.encodeBase64String(AuthenticationToken.AuthenticationTokenSerializer.serialize(pt));
            
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set 'username'",
                            CBMutationOutputFormatterTest.mockedConfiguration.containsValue(user));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set credential token",
                            CBMutationOutputFormatterTest.mockedConfiguration.containsValue(ptStr));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set default table name",
                            CBMutationOutputFormatterTest.mockedConfiguration.containsValue(defaultTable));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set create table value",
                            CBMutationOutputFormatterTest.mockedConfiguration.containsValue("false"));
            
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set 'username'",
                            this.wasPropertySet(AccumuloOutputFormat.class.getSimpleName(), user));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set credential token",
                            this.wasPropertySet(AccumuloOutputFormat.class.getSimpleName(), ptStr));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set default table name",
                            this.wasPropertySet(AccumuloOutputFormat.class.getSimpleName(), defaultTable));
            Assert.assertTrue("CBMutationOutputFormatter#setOutputInfo failed to set create table",
                            this.wasPropertySet(AccumuloOutputFormat.class.getSimpleName(), "false"));
            
        } finally {
            
            CBMutationOutputFormatterTest.logger.info("testSetOutputInfo completed.");
        }
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            TaskAttemptContext context = null;
            
            rw.close(context);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            value.put("colf".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            value.put("colf".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            value.put("ingest-test".getBytes(), "colq".getBytes(), "hello, world!!".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
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
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
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
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
            
            String simulationKey = String.format("%s.%s.%s", AccumuloOutputFormat.class.getSimpleName(), Features.SIMULATION_MODE.getDeclaringClass()
                            .getSimpleName(), StringUtils.camelize(Features.SIMULATION_MODE.name().toLowerCase()));
            
            conf.set(simulationKey, Boolean.TRUE.toString());
            conf.setInt("AccumuloOutputFormat.GeneralOpts.LogLevel", Level.ALL.toInt());
            conf.set(ShardedDataTypeHandler.SHARD_TNAME, "test-table");
            
            TaskAttemptContext attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            
            RecordWriter<Text,Mutation> rw = uut.getRecordWriter(attempt);
            
            Assert.assertNotNull("CBMutationOutputFormatter#getRecordWriter failed to create an instance of RecordWriter", rw);
            
            Text key = new Text("test-table");
            Mutation value = new Mutation("hello, world".getBytes());
            
            String colf = "ingest-test-tmp";
            value.put(colf.getBytes(), "colq".getBytes(), "hello, world!!".getBytes());
            
            rw.write(key, value);
            
            List<String> entries = uutAppender.retrieveLogsEntries();
            
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
}

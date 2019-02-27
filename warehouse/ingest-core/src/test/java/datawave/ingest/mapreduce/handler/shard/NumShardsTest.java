package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;
import org.junit.Test;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.data.config.ingest.AccumuloHelper;

public class NumShardsTest {
    @Test
    public void testDefaultNumShards() {
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        NumShards numShards = new NumShards(conf);
        assertEquals(11, numShards.getNumShards(0));
        assertEquals(11, numShards.getNumShards(Long.MAX_VALUE));
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(11, numShards.getNumShards("20171102"));
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(11, numShards.getMaxNumShards());
        assertEquals(1, numShards.getShardCount());
    }
    
    @Test
    public void testMultipleNumShards() throws ParseException {
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION, "20170101_13,20171101_17");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        NumShards numShards = new NumShards(conf);
        assertEquals(11, numShards.getNumShards(0));
        
        long cutoff = new SimpleDateFormat("yyyyMMdd").parse("20170101").getTime();
        assertEquals(11, numShards.getNumShards(cutoff - 1));
        assertEquals(13, numShards.getNumShards(cutoff));
        assertEquals(13, numShards.getNumShards(cutoff + 1));
        assertEquals(17, numShards.getNumShards(Long.MAX_VALUE));
        
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(13, numShards.getNumShards("20170101"));
        assertEquals(13, numShards.getNumShards("20170102"));
        assertEquals(17, numShards.getNumShards("20171102"));
        
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(17, numShards.getMaxNumShards());
        assertEquals(3, numShards.getShardCount());
    }
    
    @Test
    public void testMutipleNumShardsPartiallyConfigured() throws ParseException {
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.PREFETCHED_MULTIPLE_NUMSHARDS_CONFIGURATION, "20170101_13,20171101_17");
        NumShards numShards = new NumShards(conf);
        assertEquals(11, numShards.getNumShards(0));
        assertEquals(11, numShards.getNumShards(Long.MAX_VALUE));
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(11, numShards.getNumShards("20171102"));
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(11, numShards.getMaxNumShards());
        assertEquals(1, numShards.getShardCount());
    }
    
    @Test
    public void testMultipleNumShardsError() {
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        NumShards numShards = new NumShards(conf);
        
        try {
            numShards.getNumShards(0);
            fail("Multiple numshards cache is not found and prefetched multiple shard configuration isn't set. Should result in RuntimeException.");
        } catch (RuntimeException e) {}
    }
    
    @Test
    public void testMultipleNumShardsCacheLoading() throws ParseException, IOException {
        File multipleNumShardCache = File.createTempFile("numshards", ".txt");
        multipleNumShardCache.deleteOnExit();
        
        try (BufferedWriter writer = Files.newBufferedWriter(multipleNumShardCache.toPath())) {
            writer.write("20170101_13\n");
            writer.write("20171101_17");
            writer.flush();
        }
        
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, multipleNumShardCache.getParent());
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_FILENAME, multipleNumShardCache.getName());
        NumShards numShards = new NumShards(conf);
        assertEquals(11, numShards.getNumShards(0));
        
        long cutoff = new SimpleDateFormat("yyyyMMdd").parse("20170101").getTime();
        assertEquals(11, numShards.getNumShards(cutoff - 1));
        assertEquals(13, numShards.getNumShards(cutoff));
        assertEquals(13, numShards.getNumShards(cutoff + 1));
        assertEquals(17, numShards.getNumShards(Long.MAX_VALUE));
        
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(13, numShards.getNumShards("20170101"));
        assertEquals(13, numShards.getNumShards("20170102"));
        assertEquals(17, numShards.getNumShards("20171102"));
        
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(17, numShards.getMaxNumShards());
        assertEquals(3, numShards.getShardCount());
    }
    
    @Test
    public void testMultipleNumShardsCacheValidation() throws ParseException, IOException, InterruptedException {
        File multipleNumShardCache = File.createTempFile("numshards", ".txt");
        multipleNumShardCache.deleteOnExit();
        
        try (BufferedWriter writer = Files.newBufferedWriter(multipleNumShardCache.toPath())) {
            writer.write("20170101_13\n");
            writer.write("20171101_17");
            writer.flush();
        }
        
        Thread.sleep(2);
        
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, multipleNumShardCache.getParent());
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_FILENAME, multipleNumShardCache.getName());
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_TIMEOUT, "1");
        NumShards numShards = new NumShards(conf);
        
        try {
            numShards.getNumShards(0);
            fail("Multiple numshards cache is not found and prefetched multiple shard configuration isn't set. Should result in RuntimeException.");
        } catch (RuntimeException e) {}
    }
    
    @Test
    public void testMultipleNumShardsCacheParsingError() throws ParseException, IOException {
        File multipleNumShardCache = File.createTempFile("numshards", ".txt");
        multipleNumShardCache.deleteOnExit();
        
        try (BufferedWriter writer = Files.newBufferedWriter(multipleNumShardCache.toPath())) {
            writer.write("20170101_1320171101_17");
            writer.flush();
        }
        
        Configuration conf = new Configuration();
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, multipleNumShardCache.getParent());
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_FILENAME, multipleNumShardCache.getName());
        NumShards numShards = new NumShards(conf);
        
        try {
            numShards.getNumShards(0);
            fail("Multiple numshards cache file isn't valid. Should result in RuntimeException.");
        } catch (RuntimeException e) {}
    }
    
    @Test
    public void testUpdateCache() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException,
                    ParseException {
        // configure mock accumulo instance and populate with a couple of multiple numshards entries
        PasswordToken noPasswordToken = new PasswordToken();
        InMemoryInstance i = new InMemoryInstance("mock");
        Connector connector = i.getConnector("root", noPasswordToken);
        
        Configuration conf = new Configuration();
        conf.set(AccumuloHelper.USERNAME, "root");
        conf.set(AccumuloHelper.INSTANCE_NAME, "mock");
        conf.set(AccumuloHelper.PASSWORD, noPasswordToken.toString());
        conf.set(AccumuloHelper.ZOOKEEPERS, i.getZooKeepers());
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        
        connector.tableOperations().create(conf.get(ShardedDataTypeHandler.METADATA_TABLE_NAME));
        BatchWriter recordWriter = connector.createBatchWriter(conf.get(ShardedDataTypeHandler.METADATA_TABLE_NAME), new BatchWriterConfig());
        
        // write a couiple of entries for multiple numshards
        Mutation m = new Mutation(NumShards.NUM_SHARDS);
        m.put(NumShards.NUM_SHARDS_CF.toString(), "20170101_13", "");
        
        recordWriter.addMutation(m);
        
        m = new Mutation(NumShards.NUM_SHARDS);
        m.put(NumShards.NUM_SHARDS_CF.toString(), "20171101_17", "");
        
        recordWriter.addMutation(m);
        
        // invalid entry and should be ignored
        m = new Mutation(NumShards.NUM_SHARDS);
        m.put(NumShards.NUM_SHARDS_CF + "blah", "20171102_19", "");
        
        recordWriter.addMutation(m);
        
        recordWriter.close();
        
        File multipleNumShardCache = File.createTempFile("numshards", ".txt");
        multipleNumShardCache.deleteOnExit();
        
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, multipleNumShardCache.getParent());
        
        AccumuloHelper mockedAccumuloHelper = EasyMock.createMock(AccumuloHelper.class);
        mockedAccumuloHelper.setup(conf);
        EasyMock.expectLastCall();
        EasyMock.expect(mockedAccumuloHelper.getConnector()).andReturn(connector);
        EasyMock.replay(mockedAccumuloHelper);
        
        NumShards numShards = new NumShards(conf);
        
        // these should create numshards.txt file based on multiple numshards entries in mock accumulo
        numShards.setaHelper(mockedAccumuloHelper);
        numShards.updateCache();
        
        assertEquals(11, numShards.getNumShards(0));
        
        long cutoff = new SimpleDateFormat("yyyyMMdd").parse("20170101").getTime();
        assertEquals(11, numShards.getNumShards(cutoff - 1));
        assertEquals(13, numShards.getNumShards(cutoff));
        assertEquals(13, numShards.getNumShards(cutoff + 1));
        assertEquals(17, numShards.getNumShards(Long.MAX_VALUE));
        
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(13, numShards.getNumShards("20170101"));
        assertEquals(13, numShards.getNumShards("20170102"));
        assertEquals(17, numShards.getNumShards("20171102"));
        
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(17, numShards.getMaxNumShards());
        assertEquals(3, numShards.getShardCount());
    }
    
    @Test
    public void testUpdateCacheWithoutEntries() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException,
                    ParseException {
        // configure mock accumulo instance and populate with a couple of multiple numshards entries
        PasswordToken noPasswordToken = new PasswordToken();
        InMemoryInstance i = new InMemoryInstance("mock2");
        Connector connector = i.getConnector("root", noPasswordToken);
        
        Configuration conf = new Configuration();
        conf.set(AccumuloHelper.USERNAME, "root");
        conf.set(AccumuloHelper.INSTANCE_NAME, "mock2");
        conf.set(AccumuloHelper.PASSWORD, noPasswordToken.toString());
        conf.set(AccumuloHelper.ZOOKEEPERS, i.getZooKeepers());
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        
        connector.tableOperations().create(conf.get(ShardedDataTypeHandler.METADATA_TABLE_NAME));
        BatchWriter recordWriter = connector.createBatchWriter(conf.get(ShardedDataTypeHandler.METADATA_TABLE_NAME), new BatchWriterConfig());
        
        // write a couiple of entries for multiple numshards
        Mutation m = new Mutation(NumShards.NUM_SHARDS);
        m.put(NumShards.NUM_SHARDS_CF + "blah", "20171102_19", "");
        
        recordWriter.addMutation(m);
        
        recordWriter.close();
        
        File multipleNumShardCache = File.createTempFile("numshards", ".txt");
        multipleNumShardCache.deleteOnExit();
        
        conf.set(ShardIdFactory.NUM_SHARDS, "11");
        conf.set(NumShards.ENABLE_MULTIPLE_NUMSHARDS, "true");
        conf.set(NumShards.MULTIPLE_NUMSHARDS_CACHE_PATH, multipleNumShardCache.getParent());
        
        AccumuloHelper mockedAccumuloHelper = EasyMock.createMock(AccumuloHelper.class);
        mockedAccumuloHelper.setup(conf);
        EasyMock.expectLastCall();
        EasyMock.expect(mockedAccumuloHelper.getConnector()).andReturn(connector);
        EasyMock.replay(mockedAccumuloHelper);
        
        NumShards numShards = new NumShards(conf);
        
        // these should create numshards.txt file based on multiple numshards entries in mock accumulo
        numShards.setaHelper(mockedAccumuloHelper);
        numShards.updateCache();
        
        assertEquals(11, numShards.getNumShards(0));
        assertEquals(11, numShards.getNumShards(Long.MAX_VALUE));
        assertEquals(11, numShards.getNumShards(""));
        assertEquals(11, numShards.getNumShards("20171102"));
        assertEquals(11, numShards.getMinNumShards());
        assertEquals(11, numShards.getMaxNumShards());
        assertEquals(1, numShards.getShardCount());
    }
    
}

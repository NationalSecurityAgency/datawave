package nsa.datawave.ingest.mapreduce.job;

import com.google.common.io.Files;
import nsa.datawave.ingest.data.config.ingest.AccumuloHelper;
import nsa.datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class ShardedTableMapFileTest {
    public static final String PASSWORD = "123";
    public static final String USERNAME = "root";
    private static final String TABLE_NAME = "unitTestTable";
    
    @Test
    public void testWriteSplitsToFileAndReadThem() throws Exception {
        Configuration conf = new Configuration();
        
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TABLE_NAME
                        + ",shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
        
        String[] tableNames = new String[] {TABLE_NAME};
        conf.set(ShardedTableMapFile.TABLE_NAMES, StringUtils.join(",", tableNames));
        
        Map<KeyExtent,String> splits = new HashMap<>();
        splits.put(new KeyExtent(TABLE_NAME, null, null), "location1:1234"); // won't be written to splits file
        splits.put(new KeyExtent(TABLE_NAME, new Text("zEndRow"), new Text("prevEndRow")), "location2:1234");
        
        Path file = createSplitsFile(splits, conf, 1);
        conf.set(ShardedTableMapFile.SHARDED_MAP_FILE_PATHS_RAW, TABLE_NAME + "=" + file.toString());
        ShardedTableMapFile.setupFile(conf);
        TreeMap<Text,String> result = ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
        Assert.assertEquals("location2_1234", result.get(new Text("zEndRow")).toString());
        Assert.assertEquals(1, result.size());
    }
    
    @Test
    public void testWriteSplitsToAccumuloAndReadThem() throws Exception {
        Configuration conf = new Configuration();
        
        AccumuloHelper helper = new AccumuloHelper();
        MiniAccumuloCluster accumuloCluster = null;
        try {
            SortedSet sortedSet = new TreeSet();
            sortedSet.add(new Text("zEndRow"));
            accumuloCluster = createMiniAccumuloWithTestTableAndSplits(sortedSet);
            
            configureAccumuloHelper(conf, accumuloCluster);
            
            conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TABLE_NAME
                            + ",shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
            conf.set(ShardedTableMapFile.TABLE_NAMES, TABLE_NAME);
            FileSystem fs = setWorkingDirectory(conf);
            ShardedTableMapFile.setupFile(conf);
        } finally {
            if (null != accumuloCluster) {
                accumuloCluster.stop();
            }
        }
        
        TreeMap<Text,String> result = ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
        Assert.assertNotNull(result.get(new Text("zEndRow")).toString());
        Assert.assertEquals(1, result.size());
    }
    
    private MiniAccumuloCluster createMiniAccumuloWithTestTableAndSplits(SortedSet<Text> sortedSet) throws IOException, InterruptedException,
                    AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        MiniAccumuloCluster accumuloCluster;
        accumuloCluster = new MiniAccumuloCluster(Files.createTempDir(), PASSWORD);
        accumuloCluster.start();
        
        Connector connector = accumuloCluster.getConnector(USERNAME, PASSWORD);
        TableOperations tableOperations = connector.tableOperations();
        tableOperations.create(TABLE_NAME);
        tableOperations.addSplits(TABLE_NAME, sortedSet);
        
        return accumuloCluster;
    }
    
    private void configureAccumuloHelper(Configuration conf, MiniAccumuloCluster accumuloCluster) {
        AccumuloHelper.setInstanceName(conf, accumuloCluster.getInstanceName());
        AccumuloHelper.setPassword(conf, PASSWORD.getBytes());
        AccumuloHelper.setUsername(conf, USERNAME);
        AccumuloHelper.setZooKeepers(conf, accumuloCluster.getZooKeepers());
    }
    
    private Path createSplitsFile(Map<KeyExtent,String> splits, Configuration conf, int expectedNumRows) throws IOException {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, URI.create("file:///").toString());
        conf.setLong("fs.local.block.size", 32 * 1024 * 1024);
        FileSystem fs = setWorkingDirectory(conf);
        
        Path file = fs.makeQualified(new Path("splits.seq"));
        long actualCount = ShardedTableMapFile.writeSplitsFile(splits, file, conf);
        
        Assert.assertEquals("IngestJob#writeSplitsFile failed to create the expected number of rows", expectedNumRows, actualCount);
        
        Assert.assertTrue(fs.exists(file));
        return file;
    }
    
    private FileSystem setWorkingDirectory(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        File tempWorkDir = Files.createTempDir();
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        conf.set(ShardedTableMapFile.SPLIT_WORK_DIR, tempWorkDir.toString());
        return fs;
    }
    
    @Test(expected = IOException.class)
    public void testGetAllShardedTableMapFilesWithoutPath() throws Exception {
        Configuration conf = new Configuration();
        File tempWorkDir = Files.createTempDir();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, tempWorkDir.toURI().toString());
        FileSystem fs = FileSystem.get(tempWorkDir.toURI(), conf);
        fs.setWorkingDirectory(new Path(tempWorkDir.toString()));
        Path workDir = fs.makeQualified(new Path("work"));
        conf.set(ShardedTableMapFile.SPLIT_WORK_DIR, workDir.toString());
        
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, "shard_ingest_unit_test_table_1,shard_ingest_unit_test_table_2,shard_ingest_unit_test_table_3");
        
        String[] tableNames = new String[] {TABLE_NAME};
        conf.set(ShardedTableMapFile.TABLE_NAMES, StringUtils.join(",", tableNames));
        Map<String,String> shardedTableMapFilePaths = new HashMap<>();
        ShardedTableMapFile.setupFile(conf);
        ShardedTableMapFile.getShardIdToLocations(conf, TABLE_NAME);
    }
    
    @Test
    public void testWriteSplitsFileNewPath() throws Exception {
        Configuration conf = new Configuration();
        Path file = createSplitsFile(new HashMap<KeyExtent,String>(), conf, 0);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertFalse(valid);
    }
    
    @Test
    public void testWriteSplitsFileExistingPath() throws Exception {
        Map<KeyExtent,String> splits = new HashMap<>();
        Configuration conf = new Configuration();
        splits.put(new KeyExtent(), "hello, world!");
        
        Path file = createSplitsFile(splits, conf, 1);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Assert.assertEquals(Text.class, reader.getKeyClass());
        Assert.assertEquals(Text.class, reader.getValueClass());
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertTrue(valid);
        Assert.assertEquals("", key.toString());
        Assert.assertEquals("hello, world!", val.toString());
        valid = reader.next(key, val);
        Assert.assertFalse(valid);
        
    }
    
    @Test
    public void testWriteSplitsFileExistingPathMultipleKeyExtents() throws Exception {
        Map<KeyExtent,String> splits = new HashMap<>();
        splits.put(new KeyExtent(TABLE_NAME, null, null), "location1:1234"); // won't be written to splits file
        splits.put(new KeyExtent(TABLE_NAME, new Text("zEndRow"), new Text("prevEndRow")), "location2:1234");
        Configuration conf = new Configuration();
        
        Path file = createSplitsFile(splits, conf, 1);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        Assert.assertEquals(Text.class, reader.getKeyClass());
        Assert.assertEquals(Text.class, reader.getValueClass());
        Text key = new Text();
        Text val = new Text();
        boolean valid = reader.next(key, val);
        Assert.assertTrue(valid);
        Assert.assertEquals("zEndRow", key.toString());
        Assert.assertEquals("location2_1234", val.toString());
        valid = reader.next(key, val);
        Assert.assertFalse(valid);
    }
}

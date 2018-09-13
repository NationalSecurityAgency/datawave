package datawave.ingest.json.mr.input;

import java.io.File;
import java.net.URL;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;

import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.junit.Assert;
import org.junit.Test;

public class JsonRecordReaderTest {
    
    protected JsonRecordReader init(boolean parseHeaderOnly, FlattenMode mode) throws Exception {
        
        Configuration conf = null;
        TaskAttemptContext ctx = null;
        InputSplit split = null;
        File dataFile = null;
        
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/json-ingest-config.xml"));
        
        conf.set("myjson.data.json.flattener.mode", mode.name());
        conf.set("myjson.data.process.extra.fields", String.valueOf(!parseHeaderOnly));
        
        URL data = JsonRecordReaderTest.class.getResource("/input/my.json");
        Assert.assertNotNull(data);
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        
        dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        split = new FileSplit(p, 0, dataFile.length(), null);
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        JsonRecordReader reader = new JsonRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }
    
    @Test
    public void testInitialize() throws Exception {
        JsonRecordReader reader = init(true, FlattenMode.NORMAL);
        reader.close();
    }
    
    @Test
    public void testOneRecordAllFieldsNORMAL() throws Exception {
        JsonRecordReader reader = init(false, FlattenMode.NORMAL);
        reader.setInputDate(System.currentTimeMillis());
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        // All fields only should be 6, based on my.json record 1 and current config
        Assert.assertEquals(14, reader.getCurrentFields().keySet().size());
        reader.close();
    }
    
    @Test
    public void testOneRecordHeaderFieldsOnlySIMPLE() throws Exception {
        JsonRecordReader reader = init(true, FlattenMode.SIMPLE);
        reader.setInputDate(System.currentTimeMillis());
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        // "Header" fields only should be 6, based on my.json record 1 and current config
        Assert.assertEquals(6, reader.getCurrentFields().keySet().size());
        reader.close();
    }
    
    @Test
    public void testGetAllRecordsNORMAL() throws Exception {
        JsonRecordReader reader = init(false, FlattenMode.NORMAL);
        reader.setInputDate(System.currentTimeMillis());
        
        // Record 1
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(14, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(27, reader.getCurrentFields().values().size());
        // Record 2
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(18, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(29, reader.getCurrentFields().values().size());
        // Record 3
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(9, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(9, reader.getCurrentFields().values().size());
        // Record 4
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(14, reader.getCurrentFields().values().size());
        // Record 5
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(11, reader.getCurrentFields().values().size());
        
        // EOF
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
    @Test
    public void testGetAllRecordsSIMPLE() throws Exception {
        
        JsonRecordReader reader = init(false, FlattenMode.SIMPLE);
        reader.setInputDate(System.currentTimeMillis());
        
        // Record 1
        Assert.assertTrue(reader.nextKeyValue());
        RawRecordContainer rrc = reader.getEvent();
        Assert.assertNotNull(rrc);
        Assert.assertEquals(11, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(15, reader.getCurrentFields().values().size());
        // Record 2 - No recursion here, so should have only extraction of root-level primitives...
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(4, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(6, reader.getCurrentFields().values().size());
        // Record 3
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(9, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(9, reader.getCurrentFields().values().size());
        // Record 4
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(14, reader.getCurrentFields().values().size());
        // Record 5
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(11, reader.getCurrentFields().values().size());
        
        // EOF
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
    @Test
    public void testGetAllRecordsGROUPED() throws Exception {
        
        JsonRecordReader reader = init(false, FlattenMode.GROUPED);
        reader.setInputDate(System.currentTimeMillis());
        
        // Record 1
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(23, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(27, reader.getCurrentFields().values().size());
        // Record 2
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(27, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(29, reader.getCurrentFields().values().size());
        // Record 3
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(9, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(9, reader.getCurrentFields().values().size());
        // Record 4
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(14, reader.getCurrentFields().values().size());
        // Record 5
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(11, reader.getCurrentFields().values().size());
        
        // EOF
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
    @Test
    public void testGetAllRecordsGROUPED_AND_NORMAL() throws Exception {
        
        JsonRecordReader reader = init(false, FlattenMode.GROUPED_AND_NORMAL);
        reader.setInputDate(System.currentTimeMillis());
        
        // Record 1
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(23, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(27, reader.getCurrentFields().values().size());
        // Record 2
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(27, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(29, reader.getCurrentFields().values().size());
        // Record 3
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(9, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(9, reader.getCurrentFields().values().size());
        // Record 4
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(14, reader.getCurrentFields().values().size());
        // Record 5
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(11, reader.getCurrentFields().values().size());
        
        // EOF
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
}

package datawave.ingest.json.config.helper;

import com.google.common.collect.Multimap;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.json.mr.input.JsonRecordReader;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Date;

public class JsonIngestHelperTest {
    
    protected static byte[] testRecord;
    
    @BeforeClass
    public static void createTestRecord() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(" \"HEADER_DATE\" : \"2017-01-01T01:00:00Z\", ").append(" \"HEADER_ID\" : \"ID00000000001\", ")
                        .append(" \"HEADER_NUMBER\" : 10, ").append(" \"DOCUMENT_VISIBILITY\" : \"FOO&BAR\", ").append(" \"HEADER_TEXT_1\" : \"TEXT_01_01\", ")
                        .append(" \"HEADER_TEXT_2\" : \"TEXT_01_02\", ").append(" \"EXTRA_UUID_1\" : \"4e72d29968e345c2aff48d4eae803290\", ")
                        .append(" \"EXTRA_UUID_2\" : \"efa5d0348c494af0b5c7464cc0c92d75\", ")
                        .append(" \"EXTRA_TEXT\" : [ \"Extra text 1\", \"Extra text 2\", \"Extra text 3\" , { \"NESTED\" : \"Extra text 4, nested\" } ], ")
                        .append(" \"MISC_DATE\" : [ \"2017-01-01T01:01:01Z\", \"2017-02-01T02:02:01Z\", \"2017-03-01T03:03:03Z\" ], ")
                        .append(" \"MISC_TEXT\" : \"Some miscellaneous text\" ").append("}");
        testRecord = sb.toString().getBytes();
        
    }
    
    @Test
    public void testSetup() throws Exception {
        JsonIngestHelper ingestHelper = init(initConfig(FlattenMode.NORMAL));
        Assert.assertNotNull(ingestHelper.getEmbeddedHelper());
    }
    
    @Test
    public void testGetEventFieldsNORMAL() throws Exception {
        JsonIngestHelper ingestHelper = init(initConfig(FlattenMode.NORMAL));
        
        RawRecordContainer event = new RawRecordContainerImpl();
        event.setDate((new Date()).getTime());
        event.setRawData(testRecord);
        event.generateId(null);
        Assert.assertNotNull(ingestHelper.getEmbeddedHelper());
        
        Multimap<String,NormalizedContentInterface> fieldMap = ingestHelper.getEventFields(event);
        
        Assert.assertEquals(12, fieldMap.keySet().size());
        Assert.assertEquals(16, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("EXTRATEXT_NESTED"));
        Assert.assertEquals("Extra text 4, nested", fieldMap.get("EXTRATEXT_NESTED").iterator().next().getEventFieldValue());
        
        for (NormalizedContentInterface field : fieldMap.values()) {
            Assert.assertFalse(((NormalizedFieldAndValue) field).isGrouped());
        }
    }
    
    @Test
    public void testGetEventFieldsSIMPLE() throws Exception {
        JsonIngestHelper ingestHelper = init(initConfig(FlattenMode.SIMPLE));
        
        RawRecordContainer event = new RawRecordContainerImpl();
        event.setDate((new Date()).getTime());
        event.setRawData(testRecord);
        event.generateId(null);
        Assert.assertNotNull(ingestHelper.getEmbeddedHelper());
        
        Multimap<String,NormalizedContentInterface> fieldMap = ingestHelper.getEventFields(event);
        
        Assert.assertEquals(11, fieldMap.keySet().size());
        Assert.assertEquals(15, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("EXTRA_TEXT"));
        Assert.assertFalse(fieldMap.containsKey("NESTED") || fieldMap.containsKey("EXTRA_TEXT_3_NESTED"));
        
        for (NormalizedContentInterface field : fieldMap.values()) {
            Assert.assertFalse(((NormalizedFieldAndValue) field).isGrouped());
        }
    }
    
    @Test
    public void testGetEventFieldsGROUPED() throws Exception {
        JsonIngestHelper ingestHelper = init(initConfig(FlattenMode.GROUPED));
        
        RawRecordContainer event = new RawRecordContainerImpl();
        event.setDate((new Date()).getTime());
        event.setRawData(testRecord);
        event.generateId(null);
        Assert.assertNotNull(ingestHelper.getEmbeddedHelper());
        
        Multimap<String,NormalizedContentInterface> fieldMap = ingestHelper.getEventFields(event);
        
        Assert.assertEquals(12, fieldMap.keySet().size());
        Assert.assertEquals(16, fieldMap.values().size());
        Assert.assertTrue(fieldMap.containsKey("NESTED"));
        Assert.assertFalse(fieldMap.containsKey("HEADER_DATE"));
        Assert.assertTrue(fieldMap.containsKey("HEADERDATE"));
        
        for (NormalizedContentInterface field : fieldMap.values()) {
            if (((NormalizedFieldAndValue) field).isGrouped()) {
                Assert.assertEquals("NESTED", field.getIndexedFieldName());
            } else {
                Assert.assertFalse(((NormalizedFieldAndValue) field).isGrouped());
            }
        }
    }
    
    /**
     * Slightly more real-world, using the record reader to feed events to the ingest helper...
     *
     * @throws Exception
     */
    @Test
    public void testGetEventFieldsFromRecordReaderGROUPED() throws Exception {
        Configuration conf = initConfig(FlattenMode.GROUPED);
        JsonRecordReader reader = initReader(false, conf);
        reader.setInputDate(System.currentTimeMillis());
        
        JsonIngestHelper ingestHelper = init(conf);
        
        // Record 1
        Assert.assertTrue(reader.nextKeyValue());
        Multimap<String,NormalizedContentInterface> fieldMap = ingestHelper.getEventFields(reader.getEvent());
        
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(23, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(27, reader.getCurrentFields().values().size());
        
        // Record 2
        Assert.assertTrue(reader.nextKeyValue());
        fieldMap = ingestHelper.getEventFields(reader.getEvent());
        
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(27, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(29, reader.getCurrentFields().values().size());
        
        // Record 3
        Assert.assertTrue(reader.nextKeyValue());
        fieldMap = ingestHelper.getEventFields(reader.getEvent());
        
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(9, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(9, reader.getCurrentFields().values().size());
        
        // Record 4
        Assert.assertTrue(reader.nextKeyValue());
        fieldMap = ingestHelper.getEventFields(reader.getEvent());
        
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(14, reader.getCurrentFields().values().size());
        
        // Record 5
        Assert.assertTrue(reader.nextKeyValue());
        fieldMap = ingestHelper.getEventFields(reader.getEvent());
        
        Assert.assertNotNull(reader.getEvent());
        Assert.assertEquals(10, reader.getCurrentFields().keySet().size());
        Assert.assertEquals(11, reader.getCurrentFields().values().size());
        
        // EOF
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
    protected Configuration initConfig(FlattenMode mode) {
        Configuration conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/json-ingest-config.xml"));
        
        conf.set("myjson.data.json.flattener.mode", mode.name());
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        
        return conf;
    }
    
    protected JsonIngestHelper init(Configuration conf) throws Exception {
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        
        JsonIngestHelper ingestHelper = new JsonIngestHelper();
        ingestHelper.setup(conf);
        
        return ingestHelper;
    }
    
    protected JsonRecordReader initReader(boolean parseHeaderOnly, Configuration conf) throws Exception {
        
        TaskAttemptContext ctx = null;
        InputSplit split = null;
        File dataFile = null;
        
        URL data = JsonIngestHelperTest.class.getResource("/input/my.json");
        Assert.assertNotNull(data);
        
        conf.set("myjson.data.process.extra.fields", String.valueOf(!parseHeaderOnly));
        
        dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        split = new FileSplit(p, 0, dataFile.length(), null);
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        
        JsonRecordReader reader = new JsonRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }
}

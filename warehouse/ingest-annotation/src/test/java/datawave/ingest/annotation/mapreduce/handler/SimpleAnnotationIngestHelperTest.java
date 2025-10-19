package datawave.ingest.annotation.mapreduce.handler;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.annotation.mapreduce.input.SimpleAnnotationRecordReader;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;

public class SimpleAnnotationIngestHelperTest {
    protected SimpleAnnotationIngestHelper ingestHelper;
    protected SimpleAnnotationRecordReader reader;
    protected Configuration conf;
    protected TaskAttemptContext ctx = null;
    protected InputSplit split = null;

    @Before
    public void setupIngestHelper() {
        conf = new Configuration();
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/all-config.xml"));
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/annotation-ingest-config.xml"));

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        ingestHelper = new SimpleAnnotationIngestHelper();
        ingestHelper.setup(conf);

        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        reader = new SimpleAnnotationRecordReader();
    }

    protected InputSplit getSplit(String file) throws URISyntaxException, MalformedURLException {
        URL data = SimpleAnnotationIngestHelperTest.class.getResource(file);
        if (data == null) {
            File fileObj = new File(file);
            if (fileObj.exists()) {
                data = fileObj.toURI().toURL();
            }
        }
        assertNotNull("Did not find test resource", data);

        File dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        return new FileSplit(p, 0, dataFile.length(), null);
    }

    @Test
    public void testExtractedFields() throws Exception {
        split = getSplit("/input/doubleAnnotation.json");
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        Assert.assertTrue(reader.nextKeyValue());
        RawRecordContainer e = reader.getEvent();

        Assert.assertEquals("annotation", e.getDataType().outputName());
        Assert.assertNotNull(e.getRawData());
        Assert.assertTrue(reader.nextKeyValue());
        e = reader.getEvent();

        Assert.assertEquals("annotation", e.getDataType().outputName());
        Assert.assertNotNull(e.getRawData());
        Assert.assertFalse(reader.nextKeyValue());
    }
}

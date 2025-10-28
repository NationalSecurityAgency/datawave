package datawave.ingest.annotation.mapreduce.input;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

import datawave.ingest.data.TypeRegistry;

public class SimpleAnnotationRecordReaderTest {
    protected SimpleAnnotationRecordReader init(String inputData) throws Exception {
        Configuration conf = null;
        TaskAttemptContext ctx = null;
        InputSplit split = null;
        File dataFile = null;

        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/annotation-ingest-config.xml"));

        URL data = SimpleAnnotationRecordReaderTest.class.getResource(inputData);
        Assert.assertNotNull(data);

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        split = new FileSplit(p, 0, dataFile.length(), null);
        ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        SimpleAnnotationRecordReader reader = new SimpleAnnotationRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }

    @Test
    public void testSingleAnnotation() throws Exception {
        SimpleAnnotationRecordReader sarr = init("/input/singleAnnotation.json");
        Assert.assertTrue(sarr.nextKeyValue());
        Assert.assertNotNull(sarr.getEvent().getRawData());
        Assert.assertFalse(sarr.nextKeyValue());
    }

    @Test
    public void testDoubleAnnotation() throws Exception {
        SimpleAnnotationRecordReader sarr = init("/input/doubleAnnotation.json");
        Assert.assertTrue(sarr.nextKeyValue());
        Assert.assertNotNull(sarr.getEvent().getRawData());
        Assert.assertTrue(sarr.nextKeyValue());
        Assert.assertNotNull(sarr.getEvent().getRawData());
        Assert.assertFalse(sarr.nextKeyValue());
    }
}

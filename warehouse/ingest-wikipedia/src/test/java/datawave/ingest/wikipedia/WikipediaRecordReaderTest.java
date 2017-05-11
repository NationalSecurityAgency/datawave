package datawave.ingest.wikipedia;

import java.text.SimpleDateFormat;

import datawave.ingest.data.RawRecordContainer;
import datawave.data.hash.UID;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class WikipediaRecordReaderTest extends WikipediaTestBed {
    
    @Test
    public void test() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());
        
        Assert.assertTrue(reader.nextKeyValue());
        
        long time = (new SimpleDateFormat("yyyyMMdd")).parse("20130305").getTime();
        
        RawRecordContainer e = reader.getEvent();
        UID eventUID = e.getId();
        
        Assert.assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        Assert.assertEquals("enwiki", e.getDataType().outputName());
        Assert.assertEquals(time, e.getDate());
        
        Assert.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        Assert.assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        Assert.assertEquals("enwiki", e.getDataType().outputName());
        Assert.assertEquals(time, e.getDate());
        
        Assert.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
}

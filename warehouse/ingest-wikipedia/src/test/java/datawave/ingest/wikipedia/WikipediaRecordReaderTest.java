package datawave.ingest.wikipedia;

import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;

/**
 * 
 */
public class WikipediaRecordReaderTest extends WikipediaTestBed {
    
    @Test
    public void test() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());
        
        Assertions.assertTrue(reader.nextKeyValue());
        
        long time = (new SimpleDateFormat("yyyyMMdd")).parse("20130305").getTime();
        
        RawRecordContainer e = reader.getEvent();
        UID eventUID = e.getId();
        
        Assertions.assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        Assertions.assertEquals("enwiki", e.getDataType().outputName());
        Assertions.assertEquals(time, e.getDate());
        
        Assertions.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        Assertions.assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        Assertions.assertEquals("enwiki", e.getDataType().outputName());
        Assertions.assertEquals(time, e.getDate());
        
        Assertions.assertFalse(reader.nextKeyValue());
        
        reader.close();
    }
    
}

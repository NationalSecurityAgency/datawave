package datawave.ingest.wikipedia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;

import org.junit.jupiter.api.Test;

import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;

/**
 *
 */
public class WikipediaRecordReaderTest extends WikipediaTestBedJUnit5 {

    @Test
    public void test() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        assertTrue(reader.nextKeyValue());

        long time = (new SimpleDateFormat("yyyyMMdd")).parse("20130305").getTime();

        RawRecordContainer e = reader.getEvent();
        UID eventUID = e.getId();

        assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        assertEquals("enwiki", e.getDataType().outputName());
        assertEquals(time, e.getDate());

        assertTrue(reader.nextKeyValue());

        e = reader.getEvent();

        assertEquals("enwiki-20130305-pages-articles-brief.xml", e.getRawFileName());
        assertEquals("enwiki", e.getDataType().outputName());
        assertEquals(time, e.getDate());

        assertFalse(reader.nextKeyValue());

        reader.close();
    }

}

package datawave.ingest.wikipedia;

import java.util.Collection;
import java.util.Set;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * 
 */
public class WikipediaIngestHelperTest extends WikipediaTestBed {
    
    protected WikipediaIngestHelper ingestHelper;
    
    @Before
    public void setupIngestHelper() {
        ingestHelper = new WikipediaIngestHelper();
        ingestHelper.setup(conf);
    }
    
    @Test
    public void testRawEquivalence() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());
        
        Assert.assertTrue(reader.nextKeyValue());
        
        RawRecordContainer e = reader.getEvent();
        
        Multimap<String,NormalizedContentInterface> eventFields = ingestHelper.getEventFields(e);
        
        assertRawFieldsEquivalence(eventFields, this.expectedRawFieldsRecord1);
        
        Assert.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        eventFields = ingestHelper.getEventFields(e);
        
        assertRawFieldsEquivalence(eventFields, this.expectedRawFieldsRecord2);
    }
    
    protected void assertRawFieldsEquivalence(Multimap<String,NormalizedContentInterface> eventFields, Multimap<String,String> expectedRaw) {
        Assert.assertEquals(expectedRaw.size(), eventFields.size());
        for (String expectedKey : expectedRaw.keySet()) {
            Set<String> expectedValues = Sets.newHashSet(expectedRaw.get(expectedKey));
            Collection<NormalizedContentInterface> actualNCIs = eventFields.get(expectedKey);
            
            Assert.assertEquals(expectedValues.size(), actualNCIs.size());
            
            Set<String> actualValues = Sets.newHashSet();
            for (NormalizedContentInterface nci : actualNCIs) {
                actualValues.add(nci.getEventFieldValue());
            }
            
            Assert.assertEquals(expectedValues, actualValues);
        }
    }
    
    @Test
    public void testNormalizedEquivalence() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());
        
        Assert.assertTrue(reader.nextKeyValue());
        
        RawRecordContainer e = reader.getEvent();
        
        Assert.assertEquals("enwiki", e.getDataType().outputName());
        
        Multimap<String,NormalizedContentInterface> eventFields = ingestHelper.getEventFields(e);
        
        assertNormalizedFieldsEquivalence(eventFields, this.expectedNormalizedFieldsRecord1);
        
        Assert.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        eventFields = ingestHelper.getEventFields(e);
        
        assertNormalizedFieldsEquivalence(eventFields, this.expectedNormalizedFieldsRecord2);
        
    }
    
    protected void assertNormalizedFieldsEquivalence(Multimap<String,NormalizedContentInterface> eventFields, Multimap<String,String> expectedNormalized) {
        Assert.assertEquals(expectedNormalized.size(), eventFields.size());
        for (String expectedKey : expectedNormalized.keySet()) {
            Set<String> expectedValues = Sets.newHashSet(expectedNormalized.get(expectedKey));
            Collection<NormalizedContentInterface> actualNCIs = eventFields.get(expectedKey);
            
            Assert.assertEquals(expectedValues.size(), actualNCIs.size());
            
            Set<String> actualValues = Sets.newHashSet();
            for (NormalizedContentInterface nci : actualNCIs) {
                actualValues.add(nci.getIndexedFieldValue());
            }
            
            Assert.assertEquals(expectedValues, actualValues);
        }
    }
}

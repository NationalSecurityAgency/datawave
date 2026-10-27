package datawave.ingest.wikipedia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 *
 */
public class WikipediaIngestHelperTest extends WikipediaTestBedJUnit5 {

    protected WikipediaIngestHelper ingestHelper;

    @BeforeEach
    public void setupIngestHelper() {
        ingestHelper = new WikipediaIngestHelper();
        ingestHelper.setup(conf);
    }

    @Test
    public void testRawEquivalence() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        assertTrue(reader.nextKeyValue());

        RawRecordContainer e = reader.getEvent();

        Multimap<String,NormalizedContentInterface> eventFields = ingestHelper.getEventFields(e);

        assertRawFieldsEquivalence(eventFields, this.expectedRawFieldsRecord1);

        assertTrue(reader.nextKeyValue());

        e = reader.getEvent();

        eventFields = ingestHelper.getEventFields(e);

        assertRawFieldsEquivalence(eventFields, this.expectedRawFieldsRecord2);
    }

    protected void assertRawFieldsEquivalence(Multimap<String,NormalizedContentInterface> eventFields, Multimap<String,String> expectedRaw) {
        assertEquals(expectedRaw.size(), eventFields.size());
        for (String expectedKey : expectedRaw.keySet()) {
            Set<String> expectedValues = Sets.newHashSet(expectedRaw.get(expectedKey));
            Collection<NormalizedContentInterface> actualNCIs = eventFields.get(expectedKey);

            assertEquals(expectedValues.size(), actualNCIs.size());

            Set<String> actualValues = Sets.newHashSet();
            for (NormalizedContentInterface nci : actualNCIs) {
                actualValues.add(nci.getEventFieldValue());
            }

            assertEquals(expectedValues, actualValues);
        }
    }

    @Test
    public void testNormalizedEquivalence() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());

        assertTrue(reader.nextKeyValue());

        RawRecordContainer e = reader.getEvent();

        assertEquals("enwiki", e.getDataType().outputName());

        Multimap<String,NormalizedContentInterface> eventFields = ingestHelper.getEventFields(e);

        assertNormalizedFieldsEquivalence(eventFields, this.expectedNormalizedFieldsRecord1);

        assertTrue(reader.nextKeyValue());

        e = reader.getEvent();

        eventFields = ingestHelper.getEventFields(e);

        assertNormalizedFieldsEquivalence(eventFields, this.expectedNormalizedFieldsRecord2);

    }

    protected void assertNormalizedFieldsEquivalence(Multimap<String,NormalizedContentInterface> eventFields, Multimap<String,String> expectedNormalized) {
        assertEquals(expectedNormalized.size(), eventFields.size());
        for (String expectedKey : expectedNormalized.keySet()) {
            Set<String> expectedValues = Sets.newHashSet(expectedNormalized.get(expectedKey));
            Collection<NormalizedContentInterface> actualNCIs = eventFields.get(expectedKey);

            assertEquals(expectedValues.size(), actualNCIs.size());

            Set<String> actualValues = Sets.newHashSet();
            for (NormalizedContentInterface nci : actualNCIs) {
                actualValues.add(nci.getIndexedFieldValue());
            }

            assertEquals(expectedValues, actualValues);
        }
    }
}

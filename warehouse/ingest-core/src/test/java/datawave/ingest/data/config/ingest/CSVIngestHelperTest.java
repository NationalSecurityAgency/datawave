package datawave.ingest.data.config.ingest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.CSVHelper;

public class CSVIngestHelperTest {
    @Test
    public void testNullField() {
        CSVIngestHelper helper = new CSVIngestHelper();
        Multimap<String,String> fields = HashMultimap.create();
        helper.processExtraField(fields, null);
    }

    @Test
    public void testProcessFieldsSkipsMissingTrailingFields() {
        CSVIngestHelper helper = new CSVIngestHelper();
        helper.helper = new TestCSVHelper("A", "B", "C");
        HashMultimap<String,String> fields = HashMultimap.create();

        helper.processFields(fields, new String[] {"one"});

        assertTrue(fields.containsEntry("A", "one"));
        assertFalse(fields.containsKey("B"));
        assertFalse(fields.containsKey("C"));
    }

    private static class TestCSVHelper extends CSVHelper {
        private final String[] header;

        private TestCSVHelper(String... header) {
            this.header = header;
        }

        @Override
        public String[] getHeader() {
            return header;
        }

        @Override
        public String cleanEscapedMultivalueSeparators(String fieldValue) {
            return fieldValue;
        }
    }
}

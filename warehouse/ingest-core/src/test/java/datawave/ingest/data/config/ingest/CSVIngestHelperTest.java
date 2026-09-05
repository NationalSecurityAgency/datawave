package datawave.ingest.data.config.ingest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

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

    @Test
    public void testMultiValuedThresholdUsesMultiValuedAction() {
        CSVIngestHelper helper = new CSVIngestHelper();
        TestCSVHelper csvHelper = new TestCSVHelper("MULTI");
        csvHelper.multiValuedFields = Map.of("MULTI", "MULTI");
        csvHelper.multiFieldSizeThreshold = 1;
        csvHelper.thresholdAction = CSVHelper.ThresholdAction.FAIL;
        csvHelper.multiValuedThresholdAction = CSVHelper.ThresholdAction.REPLACE;
        helper.helper = csvHelper;
        HashMultimap<String,String> fields = HashMultimap.create();

        helper.processPreSplitField(fields, "MULTI", "one;two");

        assertTrue(fields.containsEntry("MULTI", "(too many)"));
        assertFalse(fields.containsEntry("MULTI", "one"));
    }

    private static class TestCSVHelper extends CSVHelper {
        private final String[] header;
        private Map<String,String> multiValuedFields = Map.of();
        private int multiFieldSizeThreshold = Integer.MAX_VALUE;
        private CSVHelper.ThresholdAction thresholdAction = CSVHelper.ThresholdAction.FAIL;
        private CSVHelper.ThresholdAction multiValuedThresholdAction = CSVHelper.ThresholdAction.FAIL;

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

        @Override
        public boolean isMultiValuedField(String fieldName) {
            return multiValuedFields.containsKey(fieldName);
        }

        @Override
        public Map<String,String> getMultiValuedFields() {
            return multiValuedFields;
        }

        @Override
        public String getEscapeSafeMultiValueSeparatorPattern() {
            return ";";
        }

        @Override
        public int getMultiFieldSizeThreshold() {
            return multiFieldSizeThreshold;
        }

        @Override
        public CSVHelper.ThresholdAction getThresholdAction() {
            return thresholdAction;
        }

        @Override
        public CSVHelper.ThresholdAction getMultiValuedThresholdAction() {
            return multiValuedThresholdAction;
        }
    }
}

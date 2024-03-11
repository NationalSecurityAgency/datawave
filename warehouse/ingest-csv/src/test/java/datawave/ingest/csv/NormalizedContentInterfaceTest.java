package datawave.ingest.csv;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;

public class NormalizedContentInterfaceTest {
    private static final Logger log = LoggerFactory.getLogger(NormalizedContentInterfaceTest.class);

    private static final String CONFIG_FILE = "config/ingest/norm-content-interface.xml";
    private static final String CSV_FILE = "/input/my-nci.csv";

    private final String DATA_NAME = "mycsv";
    private final String DATE_FIELD = "DATE_FIELD";
    private final String BAD_DATE_FIELD = "20240229 17:11:43";
    private final String FAILED_NORMALIZATION_FIELD = "FAILED_NORMALIZATION_FIELD";

    private Configuration conf;
    private FileSplit split;

    @Before
    public void setup() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource(CONFIG_FILE));

        split = new FileSplit(new Path(Objects.requireNonNull(getClass().getResource(CSV_FILE)).getPath()), 0, 200, new String[0]);
    }

    @Test
    public void testLeave() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.LEAVE.name());

        Multimap<String,NormalizedContentInterface> expectedValues = createExpectedValues();
        // for the leave policy, clear out the exception but add
        // a failed normalization field
        expectedValues.put(FAILED_NORMALIZATION_FIELD, createNormalizedContent(FAILED_NORMALIZATION_FIELD, DATE_FIELD));
        expectedValues.put(DATE_FIELD, createNormalizedContentLeave());

        test(expectedValues);
    }

    @Test
    public void testDrop() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.DROP.name());

        Multimap<String,NormalizedContentInterface> expectedValues = createExpectedValues();
        // for the drop policy, clear out the exception,
        // clear out the indexed field value and add
        // a failed normalization field
        expectedValues.put(FAILED_NORMALIZATION_FIELD, createNormalizedContent(FAILED_NORMALIZATION_FIELD, DATE_FIELD));
        expectedValues.put(DATE_FIELD, createNormalizedContentDrop());

        test(expectedValues);
    }

    @Test
    public void testFail() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.FAIL.name());

        Multimap<String,NormalizedContentInterface> expectedValues = createExpectedValues();
        // for the fail policy, leave the exception and let the
        // caller (EventMapper) fail the event
        SimpleThrowable exception = SimpleThrowable.create("Failed to normalize value as a Date: " + BAD_DATE_FIELD);
        expectedValues.put(DATE_FIELD, createNormalizedContentFail(exception));

        test(expectedValues);
    }

    private NormalizedContentInterface createNormalizedContentLeave() {
        NormalizedContentInterface fieldAndValue = createNormalizedContent(DATE_FIELD, BAD_DATE_FIELD);
        fieldAndValue.setError(null);
        return fieldAndValue;
    }

    private NormalizedContentInterface createNormalizedContentDrop() {
        NormalizedContentInterface fieldAndValue = createNormalizedContent(DATE_FIELD, BAD_DATE_FIELD, null);
        fieldAndValue.setError(null);
        return fieldAndValue;
    }

    private NormalizedContentInterface createNormalizedContentFail(Throwable throwable) {
        NormalizedContentInterface fieldAndValue = createNormalizedContent(DATE_FIELD, BAD_DATE_FIELD);
        fieldAndValue.setError(throwable);
        return fieldAndValue;
    }

    private Multimap<String,NormalizedContentInterface> createExpectedValues() {
        final String HEADER_DATE = "HEADER_DATE";
        final String HEADER_NUMBER = "HEADER_NUMBER";
        final String HEADER_ID = "HEADER_ID";
        final String HEADER_TEXT_1 = "HEADER_TEXT_1";
        final String HEADER_TEXT_2 = "HEADER_TEXT_2";

        Multimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();

        expectedValues.put(HEADER_DATE, createNormalizedContent(HEADER_DATE, "2024-02-29 12:01:47", "2024-02-29T12:01:47.000Z"));
        expectedValues.put(HEADER_NUMBER, createNormalizedContent(HEADER_NUMBER, "111", "+cE1.11"));
        expectedValues.put(HEADER_ID, createNormalizedContent(HEADER_ID, "header_one"));
        expectedValues.put(HEADER_TEXT_1, createNormalizedContent(HEADER_TEXT_1, "text one-one"));
        expectedValues.put(HEADER_TEXT_2, createNormalizedContent(HEADER_TEXT_2, "text two-one"));
        expectedValues.put(DATE_FIELD, createNormalizedContent(DATE_FIELD, "2024-02-29 12:01:47", "2024-02-29T12:01:47.000Z"));

        expectedValues.put(HEADER_DATE, createNormalizedContent(HEADER_DATE, "2024-02-29 17:11:43", "2024-02-29T17:11:43.000Z"));
        expectedValues.put(HEADER_NUMBER, createNormalizedContent(HEADER_NUMBER, "222", "+cE2.22"));
        expectedValues.put(HEADER_ID, createNormalizedContent(HEADER_ID, "header_two"));
        expectedValues.put(HEADER_TEXT_1, createNormalizedContent(HEADER_TEXT_1, "text one-two"));
        expectedValues.put(HEADER_TEXT_2, createNormalizedContent(HEADER_TEXT_2, "text two-two"));

        expectedValues.put(HEADER_DATE, createNormalizedContent(HEADER_DATE, "2024-03-01 12:01:24", "2024-03-01T12:01:24.000Z"));
        expectedValues.put(HEADER_NUMBER, createNormalizedContent(HEADER_NUMBER, "333", "+cE3.33"));
        expectedValues.put(HEADER_ID, createNormalizedContent(HEADER_ID, "header_three"));
        expectedValues.put(HEADER_TEXT_1, createNormalizedContent(HEADER_TEXT_1, "text one-three"));
        expectedValues.put(HEADER_TEXT_2, createNormalizedContent(HEADER_TEXT_2, "text two-three"));
        expectedValues.put(DATE_FIELD, createNormalizedContent(DATE_FIELD, "2024-03-01 12:01:24", "2024-03-01T12:01:24.000Z"));

        return expectedValues;
    }

    private void test(Multimap<String,NormalizedContentInterface> expectedValues) throws IOException {
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        TypeRegistry registry = TypeRegistry.getInstance(context.getConfiguration());
        Type type = registry.get(context.getConfiguration().get(DataTypeHelper.Properties.DATA_NAME));
        type.clearIngestHelper();

        Multimap<String,NormalizedContentInterface> actualValues = HashMultimap.create();

        IngestHelperInterface helper = type.getIngestHelper(context.getConfiguration());

        CSVRecordReader reader = new CSVRecordReader();
        reader.initialize(split, context);
        while (reader.nextKeyValue()) {
            Multimap<String,NormalizedContentInterface> fields = helper.getEventFields(reader.getEvent());
            fields.entries().forEach(entry -> actualValues.put(entry.getKey(), entry.getValue()));
        }
        reader.close();

        compare(actualValues, expectedValues);
    }

    private NormalizedContentInterface createNormalizedContent(String field, String value) {
        NormalizedFieldAndValue fieldAndValue = new NormalizedFieldAndValue(field, value);
        if (!field.equals(FAILED_NORMALIZATION_FIELD)) {
            fieldAndValue.setMarkings(createDefaultMarkings());
        }
        return fieldAndValue;
    }

    private NormalizedContentInterface createNormalizedContent(String field, String value, String indexValue) {
        NormalizedContentInterface fieldAndValue = createNormalizedContent(field, value);
        fieldAndValue.setIndexedFieldValue(indexValue);
        return fieldAndValue;
    }

    private Map<String,String> createDefaultMarkings() {
        Map<String,String> defaultMarkings = new HashMap<>(1);
        defaultMarkings.put("columnVisibility", "PRIVATE");
        return defaultMarkings;
    }

    private void compare(Multimap<String,NormalizedContentInterface> actualValues, Multimap<String,NormalizedContentInterface> expectedValues) {
        // need to modify any error values within the actualValues set in order to perform a simple comparison of error messages
        Multimap<String,NormalizedContentInterface> values = HashMultimap.create();
        actualValues.entries().forEach(entry -> {
            NormalizedContentInterface nci;
            if (entry.getValue().getError() == null) {
                nci = entry.getValue();
            } else {
                NormalizedContentInterface temp = new NormalizedFieldAndValue(entry.getValue());
                temp.setError(SimpleThrowable.create(temp.getError().getMessage()));
                nci = temp;
            }
            values.put(entry.getKey(), nci);
        });

        Multimap<String,NormalizedContentInterface> actualToExpectedDiff = Multimaps.filterEntries(values,
                        entry -> !expectedValues.containsEntry(entry.getKey(), entry.getValue()));
        Multimap<String,NormalizedContentInterface> expectedToActualDiff = Multimaps.filterEntries(expectedValues,
                        entry -> !values.containsEntry(entry.getKey(), entry.getValue()));

        if (!actualToExpectedDiff.isEmpty()) {
            log.error("Actual value difference exists");
            logDiffs(actualToExpectedDiff);
        }
        if (!expectedToActualDiff.isEmpty()) {
            log.error("Expected value difference exists");
            logDiffs(expectedToActualDiff);
        }

        Assert.assertFalse("The actual normalized results do not mach expected results.", !actualToExpectedDiff.isEmpty() || !expectedToActualDiff.isEmpty());
    }

    private void logDiffs(Multimap<String,NormalizedContentInterface> diff) {
        diff.entries().forEach(entry -> {
            log.error("Key: " + entry.getKey());
            log.error("Event Field Name: " + entry.getValue().getEventFieldName() + " Value: " + entry.getValue().getEventFieldValue());
            log.error("Indexed Field Name: " + entry.getValue().getIndexedFieldName() + " Value: " + entry.getValue().getIndexedFieldValue());
            if (entry.getValue().getError() != null) {
                log.error("Error: " + entry.getValue().getError().getMessage());
            }
        });
    }

    private static class SimpleThrowable extends Throwable {
        public static SimpleThrowable create(String message) {
            return new SimpleThrowable(message, null, true, false);
        }

        protected SimpleThrowable(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }

        @Override
        public int hashCode() {
            return this.getMessage().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SimpleThrowable) {
                return this.getMessage().equals(((SimpleThrowable) obj).getMessage());
            }
            return super.equals(obj);
        }
    }
}

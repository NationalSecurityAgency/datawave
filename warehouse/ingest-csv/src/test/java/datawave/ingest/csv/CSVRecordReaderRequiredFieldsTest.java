package datawave.ingest.csv;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.policy.IngestPolicyEnforcer;

public class CSVRecordReaderRequiredFieldsTest {
    private static final String DATA_TYPE = "mycsv";
    private static final String ALL_CONFIG_FILE = "config/ingest/all-config.xml";
    private static final String CSV_CONFIG_FILE = "config/ingest/csv-ingest-config.xml";

    private Configuration conf;
    private CSVRecordReader reader;

    @Before
    public void setup() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource(ALL_CONFIG_FILE));
        conf.addResource(ClassLoader.getSystemResource(CSV_CONFIG_FILE));
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DATA_TYPE + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, "");
        conf.set(DATA_TYPE + CSVHelper.DATA_HEADER_ENABLED, "false");
        conf.set(DATA_TYPE + CSVHelper.PROCESS_EXTRA_FIELDS, "true");
        conf.setStrings(DATA_TYPE + CSVHelper.REQUIRED_FIELDS, "REQUIRED");

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        reader = new CSVRecordReader();
        reader.setup(conf);
    }

    @After
    public void tearDown() {
        TypeRegistry.reset();
    }

    @Test
    public void testMissingRequiredFieldInNameValueModeAddsError() {
        // Verify required fields are checked even when the field is absent from name=value input.
        RawRecordContainer event = read("EVENT_ID=test.a2025001,OPTIONAL=value");

        assertTrue(event.getErrors().toString(), event.getErrors().contains(RawDataErrorNames.MISSING_DATA_ERROR));
    }

    @Test
    public void testPresentRequiredFieldInNameValueModeDoesNotAddError() {
        // Verify present required name=value fields satisfy the required-field check.
        RawRecordContainer event = read("EVENT_ID=test.a2025001,OPTIONAL=value,REQUIRED=value");

        assertFalse(event.getErrors().toString(), event.getErrors().contains(RawDataErrorNames.MISSING_DATA_ERROR));
    }

    private RawRecordContainer read(String rawData) {
        reader.setCurrentValue(new Text(rawData));
        return reader.getEvent();
    }
}

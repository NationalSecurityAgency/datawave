package datawave.ingest.csv.config.helper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

public class ExtendedCSVHelperTest {

    @Test
    public void testSetupResetsStateWhenHelperIsReused() throws Exception {
        // Verify a second setup does not retain state owned by ExtendedCSVHelper.
        Configuration conf = new Configuration();
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/all-config.xml"));
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/csv-ingest-config.xml"));
        conf.set("data.name.override", "datanameoverride");
        conf.setBoolean("mycsv" + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_DOWNCASE, true);
        conf.setStrings("mycsv" + ExtendedCSVHelper.Properties.IGNORED_FIELDS, "IGNORED");

        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        ExtendedCSVHelper helper = new ExtendedCSVHelper();
        helper.setup(conf);

        assertEquals("mycsv", helper.getType().typeName());
        assertEquals("csv", helper.getType().outputName());

        assertEquals("EVENT_ID", helper.getEventIdFieldName());
        assertTrue(helper.getEventIdDowncase());
        assertArrayEquals(new String[] {"IGNORED"}, helper.getIgnoredFields());

        assertEquals(11, helper.getHeader().length);

        assertEquals(",", helper.getSeparator());

        assertEquals(";", helper.getMultiValueSeparator());
        assertEquals("(?<!\\\\)\\Q;\\E", helper.getEscapeSafeMultiValueSeparatorPattern());

        assertEquals(1, helper.getSecurityMarkingFieldDomainMap().size());

        assertTrue((helper.getMultiValuedFields().size() + helper.getMultiValuedFieldsDisallowlist().size()) > 0);

        assertFalse(helper.getParsers().isEmpty());
        assertEquals(1, helper.getParsers().size());

        Configuration second = new Configuration();
        second.set(DataTypeHelper.Properties.DATA_NAME, "mycsv");
        second.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        second.setStrings("mycsv" + CSVHelper.DATA_HEADER, "FIELD");
        second.set("mycsv" + CSVHelper.DATA_SEP, ",");

        helper.setup(second);

        assertFalse(helper.getEventIdDowncase());
        assertFalse(helper.getSecurityMarkingFieldDomainMap().containsKey("SECURITY_MARKING"));
        assertArrayEquals(new String[0], helper.getIgnoredFields());
        assertTrue(helper.getParsers().isEmpty());
    }
}

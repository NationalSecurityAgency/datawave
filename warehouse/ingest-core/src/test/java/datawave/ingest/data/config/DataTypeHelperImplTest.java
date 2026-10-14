package datawave.ingest.data.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.TypeRegistry;
import datawave.policy.IngestPolicyEnforcer;

public class DataTypeHelperImplTest {

    private Configuration conf;

    @BeforeEach
    public void setup() {
        conf = new Configuration();
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
    }

    @Test
    public void testInvalidConfig() {
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        assertThrows(IllegalArgumentException.class, () -> helper.setup(conf));
    }

    @Test
    public void testValidConfig() {
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        assertNotNull(configStream);
        conf.addResource(configStream);
        assertEquals("fake", conf.get("data.name"));
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);

        assertTrue(helper.fieldsToDowncase.contains("md5"));
        assertTrue(helper.fieldsToDowncase.contains("sha1"));
        assertTrue(helper.fieldsToDowncase.contains("sha256"));

        assertEquals("abcde", helper.clean("MD5", "ABCDE"));
    }

    @Test
    public void testSetupRebuildsDowncaseFields() {
        // Verify repeated setup replaces inherited downcase fields with the current configuration.
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        assertNotNull(configStream);
        conf.addResource(configStream);
        conf.set("fake" + DataTypeHelper.Properties.DOWNCASE_FIELDS, "one,two,three,FOUR");
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);

        assertTrue(helper.fieldsToDowncase.contains("one"));
        assertTrue(helper.fieldsToDowncase.contains("two"));
        assertTrue(helper.fieldsToDowncase.contains("three"));

        assertEquals("abcde", helper.clean("THREE", "ABCDE"));
        assertEquals("abcde", helper.clean("FOUR", "ABCDE"));

        conf.set("fake" + DataTypeHelper.Properties.DOWNCASE_FIELDS, "five");
        helper.setup(conf);

        assertEquals("ABCDE", helper.clean("THREE", "ABCDE"));
        assertEquals("abcde", helper.clean("FIVE", "ABCDE"));
    }
}

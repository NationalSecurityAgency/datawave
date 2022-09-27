package datawave.ingest.data.config;

import datawave.ingest.data.TypeRegistry;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

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
        Assertions.assertThrows(IllegalArgumentException.class, () -> helper.setup(conf));
    }
    
    @Test
    public void testValidConfig() throws Exception {
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        Assertions.assertNotNull(configStream);
        conf.addResource(configStream);
        Assertions.assertEquals(conf.get("data.name"), "fake");
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);
        
        Assertions.assertTrue(helper.fieldsToDowncase.contains("md5"));
        Assertions.assertTrue(helper.fieldsToDowncase.contains("sha1"));
        Assertions.assertTrue(helper.fieldsToDowncase.contains("sha256"));
        
        Assertions.assertEquals("abcde", helper.clean("MD5", "ABCDE"));
    }
    
    @Test
    public void testDowncaseFields() throws Exception {
        InputStream configStream = getClass().getResourceAsStream("/fake-datatype-config.xml");
        Assertions.assertNotNull(configStream);
        conf.addResource(configStream);
        conf.set("fake" + DataTypeHelper.Properties.DOWNCASE_FIELDS, "one,two,three,FOUR");
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        DataTypeHelperImpl helper = new DataTypeHelperImpl();
        helper.setup(conf);
        
        Assertions.assertTrue(helper.fieldsToDowncase.contains("one"));
        Assertions.assertTrue(helper.fieldsToDowncase.contains("two"));
        Assertions.assertTrue(helper.fieldsToDowncase.contains("three"));
        
        Assertions.assertEquals("abcde", helper.clean("THREE", "ABCDE"));
        Assertions.assertEquals("abcde", helper.clean("FOUR", "ABCDE"));
    }
}

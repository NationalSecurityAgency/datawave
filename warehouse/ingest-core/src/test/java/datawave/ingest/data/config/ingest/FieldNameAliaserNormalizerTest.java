package datawave.ingest.data.config.ingest;

import java.util.HashMap;
import java.util.Map;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper.Properties;

import datawave.util.TypeRegistryTestSetup;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FieldNameAliaserNormalizerTest {
    
    private Configuration conf;
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.set(Properties.DATA_NAME, "test");
        conf.set("test" + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        TypeRegistryTestSetup.resetTypeRegistry(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBadAliasConfig() throws Exception {
        conf.set("test.data.category.index.aliases.enabled", "true");
        conf.set("test.data.category.index.aliases", "totalnonsense");
        
        FieldNameAliaserNormalizer fnan = new FieldNameAliaserNormalizer();
        fnan.setup(TypeRegistry.getType("test"), conf);
    }
    
    @Test
    public void testSetup() {
        conf.set("test.data.category.index.aliases.enabled", "true");
        conf.set("test.data.category.index.aliases", "FOO_1:BAR_1,BAR_2;FOO_2:BAR_3");
        conf.set("test.data.category.field.aliases", "HELLOWORLD:HELLO_WORLD");
        
        FieldNameAliaserNormalizer fnan = new FieldNameAliaserNormalizer();
        fnan.setup(TypeRegistry.getType("test"), conf);
        
        Map<String,String> expectedFieldAliases = new HashMap<>();
        expectedFieldAliases.put("HELLOWORLD", "HELLO_WORLD");
        
        Assert.assertEquals(expectedFieldAliases, fnan.getAliases());
        Assert.assertEquals(2, fnan.getIndexAliases("FOO_1").size());
        Assert.assertEquals(1, fnan.getIndexAliases("FOO_2").size());
    }
    
    @Test
    public void testNormalization() {
        
        FieldNameAliaserNormalizer fnan = new FieldNameAliaserNormalizer();
        fnan.setup(TypeRegistry.getType("test"), conf);
        
        String testFieldName = "HeLLo_wOrLd";
        String normalizedFieldName = fnan.normalizeAndAlias(testFieldName);
        
        Assert.assertEquals("HELLO_WORLD", normalizedFieldName);
        
        testFieldName = "Bar_3";
        
        normalizedFieldName = fnan.normalizeAndAlias(testFieldName);
        
        Assert.assertEquals("BAR_3", normalizedFieldName);
    }
    
}

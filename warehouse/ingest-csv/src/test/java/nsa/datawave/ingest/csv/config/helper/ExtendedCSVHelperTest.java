package nsa.datawave.ingest.csv.config.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import nsa.datawave.ingest.data.TypeRegistry;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ExtendedCSVHelperTest {
    
    @Test
    public void testValidConfig() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/all-config.xml"));
        conf.addResource(this.getClass().getClassLoader().getResource("config/ingest/csv-ingest-config.xml"));
        conf.set("data.name.override", "datanameoverride");
        
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        
        ExtendedCSVHelper helper = new ExtendedCSVHelper();
        helper.setup(conf);
        
        assertTrue(helper.getType().typeName().equals("mycsv"));
        assertTrue(helper.getType().outputName().equals("csv"));
        
        assertEquals("EVENT_ID", helper.getEventIdFieldName());
        
        assertEquals(11, helper.getHeader().length);
        
        assertEquals(",", helper.getSeparator());
        
        assertEquals(";", helper.getMultiValueSeparator());
        
        assertTrue(helper.getSecurityMarkingFieldDomainMap().size() == 1);
        
        assertTrue((helper.getMultiValuedFields().size() + helper.getMultiValuedFieldsBlacklist().size()) > 0);
        
        assertFalse(helper.getParsers().isEmpty());
        assertTrue(helper.getParsers().size() == 1);
    }
}

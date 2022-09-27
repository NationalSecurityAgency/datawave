package datawave.ingest.csv.config.helper;

import datawave.ingest.data.TypeRegistry;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        
        assertEquals("mycsv", helper.getType().typeName());
        assertEquals("csv", helper.getType().outputName());
        
        assertEquals("EVENT_ID", helper.getEventIdFieldName());
        
        assertEquals(11, helper.getHeader().length);
        
        assertEquals(",", helper.getSeparator());
        
        assertEquals(";", helper.getMultiValueSeparator());
        assertEquals("(?<!\\\\);", helper.getEscapeSafeMultiValueSeparatorPattern());
        
        assertEquals(1, helper.getSecurityMarkingFieldDomainMap().size());
        
        assertTrue((helper.getMultiValuedFields().size() + helper.getMultiValuedFieldsBlacklist().size()) > 0);
        
        assertFalse(helper.getParsers().isEmpty());
        assertEquals(1, helper.getParsers().size());
    }
}

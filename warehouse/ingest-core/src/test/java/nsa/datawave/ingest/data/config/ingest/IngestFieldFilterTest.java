package nsa.datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import nsa.datawave.ingest.data.Type;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IngestFieldFilterTest {
    
    private Type dataType = new Type("dt1", null, null, null, 10, null);
    private Configuration conf;
    private IngestFieldFilter filter = new IngestFieldFilter(dataType);
    private Multimap<String,String> fields;
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_SUFFIX, "FULL_NAME:FIRST_NAME,FULL_NAME:LAST_NAME,DATE_OF_BIRTH:AGE");
        filter.setup(conf);
        
        fields = HashMultimap.create();
    }
    
    @Test
    public void shouldDropAgeIfDOBPresent() {
        fields.put("AGE", "50");
        fields.put("DATE_OF_BIRTH", "50 years ago in a galaxy far, far away");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "DATE_OF_BIRTH");
        assertFieldDropped(fields, "AGE");
    }
    
    @Test
    public void shouldDropBothFirstAndLastNameIfFullNamePresent() {
        fields.put("FIRST_NAME", "Tommy");
        fields.put("LAST_NAME", "Test");
        fields.put("FULL_NAME", "Tommy Test");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "FULL_NAME");
        assertFieldDropped(fields, "FIRST_NAME");
        assertFieldDropped(fields, "LAST_NAME");
    }
    
    @Test
    public void shouldNotDropAnythingIfFieldsDoNotMatchRules() {
        fields.put("FIRST_NAME", "Tommy");
        fields.put("LAST_NAME", "Test");
        fields.put("DATE_OF_BIRTH", "50 years ago in a galaxy far, far away");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "DATE_OF_BIRTH");
        assertFieldKept(fields, "FIRST_NAME");
        assertFieldKept(fields, "LAST_NAME");
    }
    
    @Test
    public void shouldNotErrorOnIncorrectConfig() {
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_SUFFIX, "TOO:MANY:TOKENS,NAME:NICKNAME");
        
        fields.put("NAME", "Tommy Test");
        fields.put("NICKNAME", "Da Test");
        
        filter.setup(conf);
        filter.apply(fields);
        
        assertFieldKept(fields, "NAME");
        assertFieldDropped(fields, "NICKNAME");
    }
    
    private void assertFieldDropped(Multimap<String,String> fields, String field) {
        Assert.assertTrue(fields.get(field).isEmpty());
    }
    
    private void assertFieldKept(Multimap<String,String> fields, String field) {
        Assert.assertFalse(fields.get(field).isEmpty());
    }
}

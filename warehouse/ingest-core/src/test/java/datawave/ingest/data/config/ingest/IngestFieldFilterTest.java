package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IngestFieldFilterTest {
    
    private Type dataType = new Type("dt1", null, null, null, 10, null);
    private Configuration conf;
    private IngestFieldFilter filter = new IngestFieldFilter(dataType);
    private Multimap<String,Object> fields;
    
    @Before
    public void setup() {
        conf = new Configuration();
        // note I added the same filter in the opposite direction. These should be processed in order so the first one wins.
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_NAME_SUFFIX,
                        "FULL_NAME:FIRST_NAME&LAST_NAME,DATE_OF_BIRTH:AGE,FIRST_NAME&LAST_NAME:FULL_NAME");
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_VALUE_SUFFIX, "FULL_NAME:ALIAS,LAST_NAME:ALIAS,FIRST_NAME:ALIAS,A1&A2:B1&B2");
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
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnIncorrectConfig1() {
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_NAME_SUFFIX, "TOO:MANY:TOKENS,NAME:NICKNAME");
        
        filter.setup(conf);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnIncorrectConfig2() {
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_VALUE_SUFFIX, "COUNT:DOES&NOT&MATCH");
        
        filter.setup(conf);
    }
    
    @Test
    public void shouldNotFailOnIEmptyConfig() {
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_NAME_SUFFIX, "");
        conf.set(dataType.typeName() + IngestFieldFilter.FILTER_FIELD_VALUE_SUFFIX, "");
        
        filter.setup(conf);
        
        fields.put("AGE", "50");
        fields.put("DATE_OF_BIRTH", "50 years ago in a galaxy far, far away");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "DATE_OF_BIRTH");
        assertFieldKept(fields, "AGE");
    }
    
    @Test
    public void shouldDropAliasIfEqual() {
        fields.put("LAST_NAME", "Test");
        fields.put("ALIAS", "Test");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "LAST_NAME");
        assertFieldDropped(fields, "ALIAS");
    }
    
    @Test
    public void shouldNotDropAliasIfNotEqual() {
        fields.put("LAST_NAME", "Test");
        fields.put("ALIAS", "Testy");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "LAST_NAME");
        assertFieldKept(fields, "ALIAS");
    }
    
    @Test
    public void shouldDropBIfEqual() {
        fields.put("A1", "Test1");
        fields.put("A2", "Test2");
        fields.put("B1", "Test1");
        fields.put("B2", "Test2");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "A1");
        assertFieldKept(fields, "A2");
        assertFieldDropped(fields, "B1");
        assertFieldDropped(fields, "B2");
    }
    
    @Test
    public void shouldNotDropBIfNotEqual() {
        fields.put("A1", "Test1");
        fields.put("A2", "Test2");
        fields.put("B1", "Test1");
        fields.put("B2", "TestX");
        
        filter.apply(fields);
        
        assertFieldKept(fields, "A1");
        assertFieldKept(fields, "A2");
        assertFieldKept(fields, "B1");
        assertFieldKept(fields, "B2");
    }
    
    @Test
    public void shouldDropBForSameGroupOnly() {
        fields.put("A1", createGroupedValue("A1", "0", "Test1"));
        fields.put("A2", createGroupedValue("A2", "0", "Test2"));
        fields.put("B1", createGroupedValue("B1", "1", "TestX"));
        fields.put("B2", createGroupedValue("B2", "1", "Test2"));
        fields.put("B1", createGroupedValue("B1", "2", "Test1"));
        fields.put("B2", createGroupedValue("B2", "2", "Test2"));
        
        assertEquals(2, fields.get("B1").size());
        assertEquals(2, fields.get("B2").size());
        
        filter.apply(fields);
        
        assertFieldKept(fields, "A1");
        assertFieldKept(fields, "A2");
        assertFieldKept(fields, "B1");
        assertFieldKept(fields, "B2");
        assertEquals(1, fields.get("B1").size());
        assertEquals("TestX", ((GroupedNormalizedContentInterface) (fields.get("B1").iterator().next())).getEventFieldValue());
        assertEquals(1, fields.get("B2").size());
    }
    
    private GroupedNormalizedContentInterface createGroupedValue(String field, String group, String value) {
        return new NormalizedFieldAndValue(field, value, group, null);
    }
    
    private void assertFieldDropped(Multimap<String,Object> fields, String field) {
        Assert.assertTrue(fields.get(field).isEmpty());
    }
    
    private void assertFieldKept(Multimap<String,Object> fields, String field) {
        Assert.assertFalse(fields.get(field).isEmpty());
    }
    
}

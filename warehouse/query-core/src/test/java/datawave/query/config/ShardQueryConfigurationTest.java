package datawave.query.config;

import com.google.common.collect.*;
import datawave.data.type.DateType;
import datawave.data.type.StringType;
import datawave.data.type.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class ShardQueryConfigurationTest {
    
    private ShardQueryConfiguration config;
    
    @Before
    public void setUp() {
        config = ShardQueryConfiguration.create();
    }
    
    @Test
    public void testGetSetDataTypeFilter() {
        String expected = "filterA,filterB";
        Set<String> dataTypeFilters = Sets.newHashSet("filterA", "filterB");
        config.setDatatypeFilter(dataTypeFilters);
        Assert.assertEquals(expected, config.getDatatypeFilterAsString());
    }
    
    @Test
    public void testGetSetProjectFields() {
        String expected = "projectB,projectA"; // Set ordering.
        Set<String> projectFields = Sets.newHashSet("projectA", "projectB");
        config.setProjectFields(projectFields);
        Assert.assertEquals(expected, config.getProjectFieldsAsString());
    }
    
    @Test
    public void testGetSetBlacklistedFields() {
        String expected = "blacklistA,blacklistB";
        Set<String> blacklistedFields = Sets.newHashSet("blacklistA", "blacklistB");
        config.setBlacklistedFields(blacklistedFields);
        Assert.assertEquals(expected, config.getBlacklistedFieldsAsString());
    }
    
    @Test
    public void testGetSetIndexedFieldDataTypes() {
        Assert.assertEquals("", config.getIndexedFieldDataTypesAsString());
        
        Set<String> indexedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> queryFieldsDatatypes = ArrayListMultimap.create();
        queryFieldsDatatypes.put("fieldA", new DateType());
        queryFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(indexedFields);
        config.setQueryFieldsDatatypes(queryFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assert.assertEquals(expected, config.getIndexedFieldDataTypesAsString());
    }
    
    @Test
    public void testGetSetNormalizedFieldNormalizers() {
        Assert.assertEquals("", config.getNormalizedFieldNormalizersAsString());
        
        Set<String> normalizedFields = Sets.newHashSet("fieldA", "fieldB");
        Multimap<String,Type<?>> normalizedFieldsDatatypes = ArrayListMultimap.create();
        normalizedFieldsDatatypes.put("fieldA", new DateType());
        normalizedFieldsDatatypes.put("fieldB", new StringType());
        
        config.setIndexedFields(normalizedFields);
        config.setNormalizedFieldsDatatypes(normalizedFieldsDatatypes);
        
        String expected = "fieldA:datawave.data.type.DateType;fieldB:datawave.data.type.StringType;";
        Assert.assertEquals(expected, config.getNormalizedFieldNormalizersAsString());
    }
}

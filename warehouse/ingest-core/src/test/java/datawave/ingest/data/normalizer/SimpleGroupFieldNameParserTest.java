package datawave.ingest.data.normalizer;

import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class SimpleGroupFieldNameParserTest {
    
    SimpleGroupFieldNameParser parser;
    
    @Before
    public void setup() {
        parser = new SimpleGroupFieldNameParser();
    }
    
    @Test
    public void test1() {
        String field = "CANINE.PET.1";
        String expectedField = "CANINE";
        String expectedGroup = "PET";
        String expectedSubgroup = "1";
        verifyTrim(field, expectedField, expectedGroup, expectedSubgroup);
    }
    
    @Test
    public void test2() {
        String field = "MY_FIELD.PARENT1_0.PARENT2_0.FIELD_0";
        String expectedField = "MY_FIELD";
        String expectedGroup = "PARENT1.PARENT2";
        String expectedSubgroup = null;
        
        verifyTrim(field, expectedField, expectedGroup, expectedSubgroup);
    }
    
    @Test
    public void test3() {
        String field = "MY_FIELD.PARENT_1_0.PARENT_2_0.FIELD_0";
        String expectedField = "MY_FIELD";
        String expectedGroup = "PARENT_1.PARENT_2";
        String expectedUntrimmedGroup = "PARENT_1_0.PARENT_2_0.FIELD_0";
        String expectedSubgroup = null;
        
        verifyTrim(field, expectedField, expectedGroup, expectedSubgroup);
        verify(field, expectedField, expectedUntrimmedGroup, expectedSubgroup);
    }
    
    @Test
    public void test4() {
        String field = "MY_FIELD.PARENT__BANANA_123_0.FIELD_0";
        String expectedField = "MY_FIELD";
        String expectedGroup = "PARENT__BANANA_123";
        String expectedSubgroup = null;
        
        verifyTrim(field, expectedField, expectedGroup, expectedSubgroup);
    }
    
    @Test
    public void test5() {
        String field = "SO_MANY_FIELD_PERMUTATIONS.12_34_5_0.FIELD_0";
        String expectedField = "SO_MANY_FIELD_PERMUTATIONS";
        String expectedGroup = "12_34_5";
        String expectedSubgroup = null;
        
        verifyTrim(field, expectedField, expectedGroup, expectedSubgroup);
    }
    
    @Test
    public void test6() {
        String field = "CANINE.PET.2.0";
        String expectedFieldName = "CANINE";
        String expectedGroup = "PET";
        String expectedUntrimmedGroup = "PET.2.0";
        String expectedSubGroup = "0";
        
        verifyTrim(field, expectedFieldName, expectedGroup, expectedSubGroup);
        verify(field, expectedFieldName, expectedUntrimmedGroup, null);
        
    }
    
    @Test
    public void test7() {
        String field = "BANANA.0";
        String expectedFieldName = "BANANA";
        String expectedGroup = null;
        String expectedSubGroup = "0";
        
        verifyTrim(field, expectedFieldName, expectedGroup, expectedSubGroup);
        
    }
    
    @Test
    public void test8() {
        String field = "BANANA";
        String expectedFieldName = "BANANA";
        String expectedGroup = null;
        String expectedSubGroup = null;
        
        verifyTrim(field, expectedFieldName, expectedGroup, expectedSubGroup);
        
    }
    
    public void verifyTrim(String field, String expectedFieldName, String expectedGroup, String expectedSubGroup) {
        NormalizedContentInterface nci = new NormalizedFieldAndValue();
        ((NormalizedFieldAndValue) nci).setIndexedFieldName(field);
        
        GroupedNormalizedContentInterface newNci = (GroupedNormalizedContentInterface) parser.extractAndTrimFieldNameComponents(nci);
        
        Assert.assertEquals(expectedFieldName, newNci.getIndexedFieldName());
        Assert.assertEquals(expectedGroup, newNci.getGroup());
        Assert.assertEquals(expectedSubGroup, newNci.getSubGroup());
    }
    
    public void verify(String field, String expectedFieldName, String expectedGroup, String expectedSubGroup) {
        NormalizedContentInterface nci = new NormalizedFieldAndValue();
        ((NormalizedFieldAndValue) nci).setIndexedFieldName(field);
        
        GroupedNormalizedContentInterface newNci = (GroupedNormalizedContentInterface) parser.extractFieldNameComponents(nci);
        
        Assert.assertEquals(expectedFieldName, newNci.getIndexedFieldName());
        Assert.assertEquals(expectedGroup, newNci.getGroup());
        Assert.assertEquals(expectedSubGroup, newNci.getSubGroup());
    }
    
}

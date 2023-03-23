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
        String field = "PET.0";
        String expectedGroup = "PET";
        
        Assert.assertEquals(expectedGroup, SimpleGroupFieldNameParser.trimGroup(field));
    }
    
    @Test
    public void test2() {
        String field = "PARENT1_0.PARENT2_0.FIELD_0";
        String expectedGroup = "PARENT1.PARENT2";
        
        Assert.assertEquals(expectedGroup, SimpleGroupFieldNameParser.trimGroup(field));
    }
    
    @Test
    public void test3() {
        String field = "PARENT_1_0.PARENT_2_0.FIELD_0";
        String expectedGroup = "PARENT_1.PARENT_2";
        
        Assert.assertEquals(expectedGroup, SimpleGroupFieldNameParser.trimGroup(field));
    }
    
    @Test
    public void test4() {
        String field = "PARENT__BANANA_123_0.FIELD_0";
        String expectedGroup = "PARENT__BANANA_123";
        
        Assert.assertEquals(expectedGroup, SimpleGroupFieldNameParser.trimGroup(field));
    }
    
    @Test
    public void test5() {
        String field = "12_34_5_0.FIELD_0";
        String expectedGroup = "12_34_5";
        
        Assert.assertEquals(expectedGroup, SimpleGroupFieldNameParser.trimGroup(field));
    }
    
    @Test
    public void test6() {
        String field = "CANINE.PET.2.0";
        String expectedFieldName = "CANINE";
        String expectedGroup = "PET";
        String expectedSubGroup = "0";
        
        verify(field, expectedFieldName, expectedGroup, expectedSubGroup);
        
    }
    
    @Test
    public void test7() {
        String field = "MY_FIELD_NAME.PARENT_1_1.PARENT_2_0.NAME_0";
        String expectedFieldName = "MY_FIELD_NAME";
        String expectedGroup = "PARENT_1.PARENT_2";
        String expectedSubGroup = null;
        
        verify(field, expectedFieldName, expectedGroup, expectedSubGroup);
        
    }
    
    @Test
    public void test8() {
        String field = "BANANA.0";
        String expectedFieldName = "BANANA";
        String expectedGroup = null;
        String expectedSubGroup = "0";
        
        verify(field, expectedFieldName, expectedGroup, expectedSubGroup);
        
    }

    @Test
    public void test9() {
        String field = "BANANA";
        String expectedFieldName = "BANANA";
        String expectedGroup = null;
        String expectedSubGroup = null;

        verify(field, expectedFieldName, expectedGroup, expectedSubGroup);

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

package datawave.query.cardinality;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import datawave.query.model.QueryModel;
import datawave.webservice.model.Direction;
import datawave.webservice.model.FieldMapping;
import datawave.webservice.model.Model;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCardinalityConfiguration {
    
    private QueryModel QUERY_MODEL = null;
    Map<String,String> reverseMap = null;
    
    @Before
    public void init() throws Exception {
        
        URL mUrl = TestCardinalityConfiguration.class.getResource("/models/CardinalityModel.xml");
        JAXBContext ctx = JAXBContext.newInstance(datawave.webservice.model.Model.class);
        Unmarshaller u = ctx.createUnmarshaller();
        Model MODEL = (datawave.webservice.model.Model) u.unmarshal(mUrl);
        
        QUERY_MODEL = new QueryModel();
        for (FieldMapping mapping : MODEL.getFields()) {
            if (mapping.getDirection().equals(Direction.FORWARD)) {
                QUERY_MODEL.addTermToModel(mapping.getModelFieldName(), mapping.getFieldName());
            } else {
                QUERY_MODEL.addTermToReverseModel(mapping.getFieldName(), mapping.getModelFieldName());
            }
        }
        
        reverseMap = new HashMap<>();
        reverseMap.put("FIELD_1", "UUID");
        reverseMap.put("FIELD_3", "R_LABEL");
        reverseMap.put("FIELD_4", "FIELD_4B");
        reverseMap.put("FIELD_2", "PROTOCOL");
    }
    
    private Set<String> asSet(String[] fields) {
        Set<String> set = new HashSet<>();
        set.addAll(Arrays.asList(fields));
        return set;
    }
    
    @Test
    public void testRemoveCardinalityFieldsFromBlacklist1() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));
        
        // FIELD_2A is in the forward AND reverse model for field FIELD_2
        Set<String> originalBlacklistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_2A", "FIELD3"});
        Set<String> revisedBlacklist = config.getRevisedBlacklistFields(QUERY_MODEL, originalBlacklistFieldsSet);
        
        Assert.assertEquals(3, revisedBlacklist.size());
        Assert.assertFalse(revisedBlacklist.contains("FIELD_2A"));
    }
    
    @Test
    public void testRemoveCardinalityFieldsFromBlacklist2() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));
        
        // FIELD_2B is only in the forward model for field FIELD_2
        Set<String> originalBlacklistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_2B", "FIELD3"});
        Set<String> revisedBlacklist = config.getRevisedBlacklistFields(QUERY_MODEL, originalBlacklistFieldsSet);
        
        Assert.assertEquals(3, revisedBlacklist.size());
        Assert.assertFalse(revisedBlacklist.contains("FIELD_2B"));
    }
    
    @Test
    public void testRemoveCardinalityFieldsFromBlacklist3() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));
        
        // FIELD_2B is only in the forward model for field FIELD_2
        // FIELD_3A is only in the forward model for FIELD_3
        // FIELD_3 included twice -- once directly and once by model
        Set<String> originalBlacklistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_3", "FIELD_2B", "FIELD_3A", "FIELD3"});
        Set<String> revisedBlacklist = config.getRevisedBlacklistFields(QUERY_MODEL, originalBlacklistFieldsSet);
        
        Assert.assertEquals(3, revisedBlacklist.size());
        Assert.assertFalse(revisedBlacklist.contains("FIELD_2B"));
        Assert.assertFalse(revisedBlacklist.contains("FIELD_3A"));
        Assert.assertFalse(revisedBlacklist.contains("FIELD_3"));
    }
    
    @Test
    public void testAddCardinalityFieldsToProjectFields1() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));
        
        Set<String> originalProjectFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD3"});
        Set<String> revisedProjectFields = config.getRevisedProjectFields(QUERY_MODEL, originalProjectFieldsSet);
        
        Assert.assertEquals(7, revisedProjectFields.size());
        Assert.assertTrue(revisedProjectFields.contains("FIELD_2A"));
        Assert.assertTrue(revisedProjectFields.contains("FIELD_3C"));
        Assert.assertTrue(revisedProjectFields.contains("FIELD_4A"));
        Assert.assertTrue(revisedProjectFields.contains("UUID"));
    }
    
    @Test
    public void testAddCardinalityFieldsToProjectFields2() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));
        
        Set<String> originalProjectFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD3"});
        Set<String> revisedProjectFields = config.getRevisedProjectFields(QUERY_MODEL, originalProjectFieldsSet);
        
        Assert.assertEquals(6, revisedProjectFields.size());
        Assert.assertTrue(revisedProjectFields.contains("FIELD_2A"));
        Assert.assertTrue(revisedProjectFields.contains("FIELD_3C"));
        Assert.assertTrue(revisedProjectFields.contains("UUID"));
    }
    
    @Test
    public void testAddCardinalityFieldsToProjectFieldsNoModel() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));
        
        Set<String> originalProjectFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD3"});
        Set<String> revisedProjectFields = config.getRevisedProjectFields(null, originalProjectFieldsSet);
        
        Assert.assertEquals(6, revisedProjectFields.size());
        Assert.assertTrue(revisedProjectFields.contains("FIELD_1"));
        Assert.assertTrue(revisedProjectFields.contains("FIELD_2"));
        Assert.assertTrue(revisedProjectFields.contains("FIELD_3"));
    }
    
    @Test
    public void testAddCardinalityFieldsToProjectFieldsNoWhitelist() {
        
        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));
        
        Set<String> originalProjectFieldsSet = Collections.emptySet();
        Set<String> revisedProjectFields = config.getRevisedProjectFields(QUERY_MODEL, originalProjectFieldsSet);
        
        Assert.assertEquals(0, revisedProjectFields.size());
    }
    
}

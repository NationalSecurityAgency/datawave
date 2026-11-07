package datawave.query.cardinality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.model.Direction;
import datawave.query.model.FieldMapping;
import datawave.query.model.QueryModel;
import datawave.webservice.model.Model;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Unmarshaller;

public class TestCardinalityConfiguration {

    private QueryModel QUERY_MODEL = null;
    Map<String,String> reverseMap = null;

    @BeforeEach
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
    public void testRemoveCardinalityFieldsFromDisallowlist1() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));

        // FIELD_2A is in the forward AND reverse model for field FIELD_2
        Set<String> originalDisallowlistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_2A", "FIELD3"});
        Set<String> revisedDisallowlist = config.getRevisedDisallowlistFields(QUERY_MODEL, originalDisallowlistFieldsSet);

        assertEquals(3, revisedDisallowlist.size());
        assertFalse(revisedDisallowlist.contains("FIELD_2A"));
    }

    @Test
    public void testRemoveCardinalityFieldsFromDisalllowlist2() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));

        // FIELD_2B is only in the forward model for field FIELD_2
        Set<String> originalDisallowlistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_2B", "FIELD3"});
        Set<String> revisedDisallowlist = config.getRevisedDisallowlistFields(QUERY_MODEL, originalDisallowlistFieldsSet);

        assertEquals(3, revisedDisallowlist.size());
        assertFalse(revisedDisallowlist.contains("FIELD_2B"));
    }

    @Test
    public void testRemoveCardinalityFieldsFromDisallowlist3() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE", "R_LABEL",
                "FIELD_4B", "QUERY_USER|PROTOCOL"}));

        // FIELD_2B is only in the forward model for field FIELD_2
        // FIELD_3A is only in the forward model for FIELD_3
        // FIELD_3 included twice -- once directly and once by model
        Set<String> originalDisallowlistFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD_3", "FIELD_2B", "FIELD_3A", "FIELD3"});
        Set<String> revisedDisallowlist = config.getRevisedDisallowlistFields(QUERY_MODEL, originalDisallowlistFieldsSet);

        assertEquals(3, revisedDisallowlist.size());
        assertFalse(revisedDisallowlist.contains("FIELD_2B"));
        assertFalse(revisedDisallowlist.contains("FIELD_3A"));
        assertFalse(revisedDisallowlist.contains("FIELD_3"));
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

        assertEquals(7, revisedProjectFields.size());
        assertTrue(revisedProjectFields.contains("FIELD_2A"));
        assertTrue(revisedProjectFields.contains("FIELD_3C"));
        assertTrue(revisedProjectFields.contains("FIELD_4A"));
        assertTrue(revisedProjectFields.contains("UUID"));
    }

    @Test
    public void testAddCardinalityFieldsToProjectFields2() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));

        Set<String> originalProjectFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD3"});
        Set<String> revisedProjectFields = config.getRevisedProjectFields(QUERY_MODEL, originalProjectFieldsSet);

        assertEquals(6, revisedProjectFields.size());
        assertTrue(revisedProjectFields.contains("FIELD_2A"));
        assertTrue(revisedProjectFields.contains("FIELD_3C"));
        assertTrue(revisedProjectFields.contains("UUID"));
    }

    @Test
    public void testAddCardinalityFieldsToProjectFieldsNoModel() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));

        Set<String> originalProjectFieldsSet = asSet(new String[] {"FIELD1", "FIELD2", "FIELD3"});
        Set<String> revisedProjectFields = config.getRevisedProjectFields(null, originalProjectFieldsSet);

        assertEquals(6, revisedProjectFields.size());
        assertTrue(revisedProjectFields.contains("FIELD_1"));
        assertTrue(revisedProjectFields.contains("FIELD_2"));
        assertTrue(revisedProjectFields.contains("FIELD_3"));
    }

    @Test
    public void testAddCardinalityFieldsToProjectFieldsNoAllowlist() {

        CardinalityConfiguration config = new CardinalityConfiguration();
        config.setCardinalityUidField("UUID");
        config.setCardinalityFieldReverseMapping(reverseMap);
        config.setCardinalityFields(asSet(new String[] {"R_LABEL", "QUERY_USER|PROTOCOL"}));

        Set<String> originalProjectFieldsSet = Collections.emptySet();
        Set<String> revisedProjectFields = config.getRevisedProjectFields(QUERY_MODEL, originalProjectFieldsSet);

        assertEquals(0, revisedProjectFields.size());
    }

}

package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

class WhindexFieldIngestHelperTest {

    /**
     * Test that when no whindex rules are configured, the WhindexFieldIngestHelper's internal
     * mapping is empty.
     */
    @Test
    void testEmptyWhindexConfiguration() {
        // Create a new empty configuration for the "wiki" type.
        Configuration config = new Configuration();
        config.set("wiki", ""); // Setting an empty value for the wiki configuration.

        // Initialize the type and helper using the configuration.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Create an empty expected mapping.
        Multimap<String, WhindexConfig> expectedValues = LinkedListMultimap.create();

        // Verify that the valueFieldsToWhindexConfigs() mapping is empty.
        Assertions.assertEquals(expectedValues, wHelper.getValueFieldsToWhindexConfigs());
    }

    /**
     * Test that whindex rules are correctly parsed from the configuration when there are
     * multiple rules defined.
     */
    @Test
    void testWhindexFieldDefinitionsParsing() {
        // Create a configuration with multiple whindex rule definitions.
        Configuration config = new Configuration();

        // First rule: delete_src_field is set to true.
        config.set("wiki.whindex.rules.1.value_field", "PrimaryValueField");
        config.set("wiki.whindex.rules.1.src_field", "PrimarySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "PrimaryDestField");
        config.set("wiki.whindex.rules.1.values", "primaryTestValue1,primaryTestValue2,primaryTestValue3,primaryTestValue4");
        WhindexConfig expectedConfigA = new WhindexConfig();
        expectedConfigA.setValueField("PrimaryValueField");
        expectedConfigA.setSourceField("PrimarySourceField");
        expectedConfigA.setOverloaded(true);
        expectedConfigA.setDestField("PrimaryDestField");
        expectedConfigA.setValues(Arrays.asList("primaryTestValue1", "primaryTestValue2", "primaryTestValue3", "primaryTestValue4"));

        // Second rule: delete_src_field is set to false.
        config.set("wiki.whindex.rules.2.value_field", "SecondValueField");
        config.set("wiki.whindex.rules.2.src_field", "SecondSourceField");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "SecondDestField");
        config.set("wiki.whindex.rules.2.values", "secondTestValue1,secondTestValue2,secondTestValue3,secondTestValue4");
        WhindexConfig expectedConfigB = new WhindexConfig();
        expectedConfigB.setValueField("SecondValueField");
        expectedConfigB.setSourceField("SecondSourceField");
        expectedConfigB.setOverloaded(false);
        expectedConfigB.setDestField("SecondDestField");
        expectedConfigB.setValues(Arrays.asList("secondTestValue1", "secondTestValue2", "secondTestValue3", "secondTestValue4"));

        // Third rule: no delete_src_field specified, so assumed false.
        config.set("wiki.whindex.rules.sillyrule.value_field", "ThirdValueField");
        config.set("wiki.whindex.rules.sillyrule.src_field", "ThirdSourceField");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "ThirdDestField");
        config.set("wiki.whindex.rules.sillyrule.values", "thirdTestValue1,thirdTestValue2,thirdTestValue3,thirdTestValue4");
        WhindexConfig expectedConfigC = new WhindexConfig();
        expectedConfigC.setValueField("ThirdValueField");
        expectedConfigC.setSourceField("ThirdSourceField");
        expectedConfigC.setOverloaded(false);
        expectedConfigC.setDestField("ThirdDestField");
        expectedConfigC.setValues(Arrays.asList("thirdTestValue1", "thirdTestValue2", "thirdTestValue3", "thirdTestValue4"));

        // Setup helper using the specified configuration.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the expected mapping from valueField to the corresponding WhindexConfig.
        HashMultimap<String, WhindexConfig> expectedValues = HashMultimap.create();
        expectedValues.put("PrimaryValueField", expectedConfigA);
        expectedValues.put("SecondValueField", expectedConfigB);
        expectedValues.put("ThirdValueField", expectedConfigC);

        // Assert that the parsed whindex configurations match the expected mapping.
        Assertions.assertEquals(expectedValues, wHelper.getValueFieldsToWhindexConfigs());
    }

    /**
     * Test that getWhindexFields() correctly transforms the event map when the rule's
     * delete_src_field is specified as "true".
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldTrue() {
        // Setup configuration with a rule that has delete_src_field set to true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "PrimaryValueField");
        config.set("wiki.whindex.rules.1.src_field", "PrimarySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "PrimaryDestField");
        config.set("wiki.whindex.rules.1.values", "primaryTestValue1,primaryTestValue2,primaryTestValue3,primaryTestValue4");

        // Create the helper instance for the "wiki" type and initialize with the configuration.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build an event map containing the value field entry and its corresponding source field.
        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("PrimaryValueField", new NormalizedFieldAndValue("PrimaryValueField", "primaryTestValue1"));
        eventMap.put("PrimarySourceField", new NormalizedFieldAndValue("PrimarySourceField", "primaryTestValue1"));

        // Retrieve the output mapping after processing the event map.
        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect that the source field value is mapped to the destination field.
        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("PrimaryDestField", new NormalizedFieldAndValue("PrimarySourceField", "primaryTestValue1"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that getWhindexFields() correctly transforms the event map when the rule's
     * delete_src_field is specified as "false".
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldFalse() {
        // Setup configuration with a rule that has delete_src_field set to false.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "SecondValueField");
        config.set("wiki.whindex.rules.1.src_field", "SecondSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "SecondDestField");
        config.set("wiki.whindex.rules.1.values", "secondTestValue1,secondTestValue2,secondTestValue3,secondTestValue4");

        // Initialize the helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the event map with entries for the value field and source field.
        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("SecondValueField", new NormalizedFieldAndValue("SecondValueField", "secondTestValue1"));
        eventMap.put("SecondSourceField", new NormalizedFieldAndValue("SecondSourceField", "secondTestValue1"));

        // Process the event map.
        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect that the destination field receives the transformed value.
        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("SecondDestField", new NormalizedFieldAndValue("SecondSourceField", "secondTestValue1"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that getWhindexFields() correctly handles the case when no delete_src_field property is provided.
     * <p>
     * In this case, the helper should assume delete_src_field is false.
     * </p>
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldNotPresent() {
        // Setup configuration without providing the delete_src_field property.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.sillyrule.value_field", "ThirdValueField");
        config.set("wiki.whindex.rules.sillyrule.src_field", "ThirdSourceField");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "ThirdDestField");
        config.set("wiki.whindex.rules.sillyrule.values", "thirdTestValue1,thirdTestValue2,thirdTestValue3,thirdTestValue4");

        // Initialize the helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the event map.
        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("ThirdValueField", new NormalizedFieldAndValue("ThirdValueField", "thirdTestValue1"));
        eventMap.put("ThirdSourceField", new NormalizedFieldAndValue("ThirdSourceField", "thirdTestValue1"));

        // Process the event map.
        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect the mapping to be under the destination field since no delete_src_field is provided (assumed false).
        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("ThirdDestField", new NormalizedFieldAndValue("ThirdSourceField", "thirdTestValue1"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that isWhindexField() correctly identifies whindex fields when the rule's
     * delete_src_field is set to "true".
     * <p>
     * Only the destination field should be considered a whindex field.
     * </p>
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldTrue() {
        // Setup configuration with delete_src_field true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "PrimaryValueField");
        config.set("wiki.whindex.rules.1.src_field", "PrimarySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "PrimaryDestField");
        config.set("wiki.whindex.rules.1.values", "primaryTestValue1,primaryTestValue2,primaryTestValue3,primaryTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that the destination field is recognized as a whindex field,
        // and the source as well as the value field are not.
        Assertions.assertTrue(wHelper.isWhindexField("PrimaryDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("PrimarySourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("primaryTestValue1"));
    }

    /**
     * Test that isWhindexField() correctly identifies whindex fields when the rule's
     * delete_src_field is set to "false".
     * <p>
     * Only the destination field should be considered a whindex field.
     * </p>
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldFalse() {
        // Setup configuration with delete_src_field false.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "SecondValueField");
        config.set("wiki.whindex.rules.1.src_field", "SecondSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "SecondDestField");
        config.set("wiki.whindex.rules.1.values", "secondTestValue1,secondTestValue2,secondTestValue3,secondTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that only the destination field is identified as a whindex field.
        Assertions.assertTrue(wHelper.isWhindexField("SecondDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("SecondSourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("secondTestValue1"));
    }

    /**
     * Test that isWhindexField() works correctly when delete_src_field is not specified.
     * <p>
     * In this case, only the destination field should be identified as a whindex field.
     * </p>
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldNotPresent() {
        // Setup configuration without the delete_src_field property.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.sillyrule.value_field", "ThirdValueField");
        config.set("wiki.whindex.rules.sillyrule.src_field", "ThirdSourceField");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "ThirdDestField");
        config.set("wiki.whindex.rules.sillyrule.values", "thirdTestValue1,thirdTestValue2,thirdTestValue3,thirdTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that only the destination field is recognized as a whindex field.
        Assertions.assertTrue(wHelper.isWhindexField("ThirdDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("ThirdSourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("thirdTestValue1"));
    }

    /**
     * Test that isOverloadedWhindexField() identifies overloaded fields correctly when the rule's
     * delete_src_field is set to "true".
     * <p>
     * When delete_src_field is true, the source field should be considered overloaded.
     * </p>
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldTrue() {
        // Setup configuration with delete_src_field true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "PrimaryValueField");
        config.set("wiki.whindex.rules.1.src_field", "PrimarySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "PrimaryDestField");
        config.set("wiki.whindex.rules.1.values", "primaryTestValue1,primaryTestValue2,primaryTestValue3,primaryTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that with delete_src_field true, the source field is marked as overloaded,
        // while the destination field is not.
        Assertions.assertTrue(wHelper.isOverloadedWhindexField("PrimarySourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("PrimaryDestField"));
    }

    /**
     * Test that isOverloadedWhindexField() works correctly when delete_src_field is set to "false".
     * <p>
     * When delete_src_field is false, neither the source field nor the destination field should be marked as overloaded.
     * </p>
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldFalse() {
        // Setup configuration with delete_src_field false.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "SecondValueField");
        config.set("wiki.whindex.rules.1.src_field", "SecondSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "SecondDestField");
        config.set("wiki.whindex.rules.1.values", "secondTestValue1,secondTestValue2,secondTestValue3,secondTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that when delete_src_field is false, neither source nor destination fields are overloaded.
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("SecondSourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("SecondDestField"));
    }

    /**
     * Test that isOverloadedWhindexField() works correctly when no delete_src_field property is provided.
     * <p>
     * In this scenario, the source field is not considered overloaded.
     * </p>
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldNotPresent() {
        // Setup configuration without the delete_src_field property.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.sillyrule.value_field", "ThirdValueField");
        config.set("wiki.whindex.rules.sillyrule.src_field", "ThirdSourceField");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "ThirdDestField");
        config.set("wiki.whindex.rules.sillyrule.values", "thirdTestValue1,thirdTestValue2,thirdTestValue3,thirdTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that, without a delete_src_field property, the source field is not marked as overloaded.
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("ThirdSourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("ThirdDestField"));
    }
}

package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
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
     * Test that when no whindex rules are configured, the WhindexFieldIngestHelper's internal mapping is empty.
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
        Multimap<String,WhindexConfig> expectedValues = LinkedListMultimap.create();

        // Verify that the valueFieldsToWhindexConfigs() mapping is empty.
        Assertions.assertEquals(expectedValues, wHelper.getValueFieldsToWhindexConfigs());
    }

    /**
     * Test that whindex rules are correctly parsed from the configuration when there are multiple rules defined.
     */
    @Test
    void testWhindexFieldDefinitionsParsing() {
        // Create a configuration with multiple whindex rule definitions.
        Configuration config = new Configuration();

        // First rule: delete_src_field is set to true.
        config.set("wiki.whindex.rules.1.value_field", "xValueField");
        config.set("wiki.whindex.rules.1.src_field", "xSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "xDestField");
        config.set("wiki.whindex.rules.1.values", "xTestValue1,xTestValue2,xTestValue3,xTestValue4");
        WhindexConfig expectedConfigA = new WhindexConfig();
        expectedConfigA.setValueField("xValueField");
        expectedConfigA.setSourceField("xSourceField");
        expectedConfigA.setOverloaded(true);
        expectedConfigA.setDestField("xDestField");
        expectedConfigA.setValues(Arrays.asList("xTestValue1", "xTestValue2", "xTestValue3", "xTestValue4"));

        // y rule: delete_src_field is set to false.
        config.set("wiki.whindex.rules.2.value_field", "yValueField");
        config.set("wiki.whindex.rules.2.src_field", "ySourceField");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "yDestField");
        config.set("wiki.whindex.rules.2.values", "yTestValue1,yTestValue2,yTestValue3,yTestValue4");
        WhindexConfig expectedConfigB = new WhindexConfig();
        expectedConfigB.setValueField("yValueField");
        expectedConfigB.setSourceField("ySourceField");
        expectedConfigB.setOverloaded(false);
        expectedConfigB.setDestField("yDestField");
        expectedConfigB.setValues(Arrays.asList("yTestValue1", "yTestValue2", "yTestValue3", "yTestValue4"));

        // z rule: no delete_src_field specified, so assumed false.
        config.set("wiki.whindex.rules.namedrule.value_field", "zValueField");
        config.set("wiki.whindex.rules.namedrule.src_field", "zSourceField");
        config.set("wiki.whindex.rules.namedrule.dst_field", "zDestField");
        config.set("wiki.whindex.rules.namedrule.values", "zTestValue1,zTestValue2,zTestValue3,zTestValue4");
        WhindexConfig expectedConfigC = new WhindexConfig();
        expectedConfigC.setValueField("zValueField");
        expectedConfigC.setSourceField("zSourceField");
        expectedConfigC.setOverloaded(false);
        expectedConfigC.setDestField("zDestField");
        expectedConfigC.setValues(Arrays.asList("zTestValue1", "zTestValue2", "zTestValue3", "zTestValue4"));

        // Setup helper using the specified configuration.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the expected mapping from valueField to the corresponding WhindexConfig.
        HashMultimap<String,WhindexConfig> expectedValues = HashMultimap.create();
        expectedValues.put("xValueField", expectedConfigA);
        expectedValues.put("yValueField", expectedConfigB);
        expectedValues.put("zValueField", expectedConfigC);

        // Assert that the parsed whindex configurations match the expected mapping.
        Assertions.assertEquals(ImmutableMultimap.copyOf(expectedValues), wHelper.getValueFieldsToWhindexConfigs());
    }

    /**
     * Test that getWhindexFields() correctly transforms the event map when the rule's delete_src_field is specified as "true".
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldTrue() {
        // Setup configuration with a rule that has delete_src_field set to true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "xValueField");
        config.set("wiki.whindex.rules.1.src_field", "xSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "xDestField");
        config.set("wiki.whindex.rules.1.values", "xTestValue1,xTestValue2,xTestValue3,xTestValue4");

        // Create the helper instance for the "wiki" type and initialize with the configuration.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build an event map containing the value field entry and its corresponding source field.
        Multimap<String,NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("xValueField", new NormalizedFieldAndValue("xValueField", "xTestValue1"));
        eventMap.put("xSourceField", new NormalizedFieldAndValue("xSourceField", "xTestValue1"));

        // Retrieve the output mapping after processing the event map.
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect that the source field value is mapped to the destination field.
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("xDestField", new NormalizedFieldAndValue("xSourceField", "xTestValue1"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that getWhindexFields() correctly transforms the event map when the rule's delete_src_field is specified as "false".
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldFalse() {
        // Setup configuration with a rule that has delete_src_field set to false.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "yValueField");
        config.set("wiki.whindex.rules.1.src_field", "ySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "yDestField");
        config.set("wiki.whindex.rules.1.values", "yTestValue1,yTestValue2,yTestValue3,yTestValue4");

        // Initialize the helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the event map with entries for the value field and source field.
        Multimap<String,NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("yValueField", new NormalizedFieldAndValue("yValueField", "yTestValue1"));
        eventMap.put("ySourceField", new NormalizedFieldAndValue("ySourceField", "yTestValue1"));

        // Process the event map.
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect that the destination field receives the transformed value.
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("yDestField", new NormalizedFieldAndValue("ySourceField", "yTestValue1"));
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
        config.set("wiki.whindex.rules.namedrule.value_field", "zValueField");
        config.set("wiki.whindex.rules.namedrule.src_field", "zSourceField");
        config.set("wiki.whindex.rules.namedrule.dst_field", "zDestField");
        config.set("wiki.whindex.rules.namedrule.values", "zTestValue1,zTestValue2,zTestValue3,zTestValue4");

        // Initialize the helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the event map.
        Multimap<String,NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("zValueField", new NormalizedFieldAndValue("zValueField", "zTestValue1"));
        eventMap.put("zSourceField", new NormalizedFieldAndValue("zSourceField", "zTestValue1"));

        // Process the event map.
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect the mapping to be under the destination field since no delete_src_field is provided (assumed false).
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("zDestField", new NormalizedFieldAndValue("zSourceField", "zTestValue1"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that isWhindexField() correctly identifies whindex fields when the rule's delete_src_field is set to "true".
     * <p>
     * Only the destination field should be considered a whindex field.
     * </p>
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldTrue() {
        // Setup configuration with delete_src_field true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "xValueField");
        config.set("wiki.whindex.rules.1.src_field", "xSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "xDestField");
        config.set("wiki.whindex.rules.1.values", "xTestValue1,xTestValue2,xTestValue3,xTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that the destination field is recognized as a whindex field,
        // and the source as well as the value field are not.
        Assertions.assertTrue(wHelper.isWhindexField("xDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("xSourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("xTestValue1"));
    }

    /**
     * Test that isWhindexField() correctly identifies whindex fields when the rule's delete_src_field is set to "false".
     * <p>
     * Only the destination field should be considered a whindex field.
     * </p>
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldFalse() {
        // Setup configuration with delete_src_field false.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "yValueField");
        config.set("wiki.whindex.rules.1.src_field", "ySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "yDestField");
        config.set("wiki.whindex.rules.1.values", "yTestValue1,yTestValue2,yTestValue3,yTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that only the destination field is identified as a whindex field.
        Assertions.assertTrue(wHelper.isWhindexField("yDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("ySourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("yTestValue1"));
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
        config.set("wiki.whindex.rules.namedrule.value_field", "zValueField");
        config.set("wiki.whindex.rules.namedrule.src_field", "zSourceField");
        config.set("wiki.whindex.rules.namedrule.dst_field", "zDestField");
        config.set("wiki.whindex.rules.namedrule.values", "zTestValue1,zTestValue2,zTestValue3,zTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that only the destination field is recognized as a whindex field.
        Assertions.assertTrue(wHelper.isWhindexField("zDestField"));
        Assertions.assertFalse(wHelper.isWhindexField("zSourceField"));
        Assertions.assertFalse(wHelper.isWhindexField("zTestValue1"));
    }

    /**
     * Test that isOverloadedWhindexField() identifies overloaded fields correctly when the rule's delete_src_field is set to "true".
     * <p>
     * When delete_src_field is true, the source field should be considered overloaded.
     * </p>
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldTrue() {
        // Setup configuration with delete_src_field true.
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "xValueField");
        config.set("wiki.whindex.rules.1.src_field", "xSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "xDestField");
        config.set("wiki.whindex.rules.1.values", "xTestValue1,xTestValue2,xTestValue3,xTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that with delete_src_field true, the source field is marked as overloaded,
        // while the destination field is not.
        Assertions.assertTrue(wHelper.isOverloadedWhindexField("xSourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("xDestField"));
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
        config.set("wiki.whindex.rules.1.value_field", "yValueField");
        config.set("wiki.whindex.rules.1.src_field", "ySourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "yDestField");
        config.set("wiki.whindex.rules.1.values", "yTestValue1,yTestValue2,yTestValue3,yTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that when delete_src_field is false, neither source nor destination fields are overloaded.
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("ySourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("yDestField"));
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
        config.set("wiki.whindex.rules.namedrule.value_field", "zValueField");
        config.set("wiki.whindex.rules.namedrule.src_field", "zSourceField");
        config.set("wiki.whindex.rules.namedrule.dst_field", "zDestField");
        config.set("wiki.whindex.rules.namedrule.values", "zTestValue1,zTestValue2,zTestValue3,zTestValue4");

        // Initialize helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Verify that, without a delete_src_field property, the source field is not marked as overloaded.
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("zSourceField"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("zDestField"));
    }
}

package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultimap;
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

        // x rule: delete_src_field is set to true.
        config.set("wiki.whindex.rules.1.value_field", "xValueField");
        config.set("wiki.whindex.rules.1.src_field", "xSourceField");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "xDestField");
        config.set("wiki.whindex.rules.1.values", "xTestValue1,xTestValue2,xTestValue3,xTestValue4");
        WhindexConfig expectedConfigA = WhindexConfig.builder()
                .withValueField("xValueField")
                .withSourceField("xSourceField")
                .withOverloaded(true)
                .withDestField("xDestField")
                .withValues(Arrays.asList("xTestValue1", "xTestValue2", "xTestValue3", "xTestValue4"))
                .build();

        // y rule: delete_src_field is set to false.
        config.set("wiki.whindex.rules.2.value_field", "yValueField");
        config.set("wiki.whindex.rules.2.src_field", "ySourceField");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "yDestField");
        config.set("wiki.whindex.rules.2.values", "yTestValue1,yTestValue2,yTestValue3,yTestValue4");
        WhindexConfig expectedConfigB = WhindexConfig.builder()
                .withValueField("yValueField")
                .withSourceField("ySourceField")
                .withOverloaded(false)
                .withDestField("yDestField")
                .withValues(Arrays.asList("yTestValue1", "yTestValue2", "yTestValue3", "yTestValue4"))
                .build();

        // z rule: no delete_src_field specified, so assumed false.
        config.set("wiki.whindex.rules.namedrule.value_field", "zValueField");
        config.set("wiki.whindex.rules.namedrule.src_field", "zSourceField");
        config.set("wiki.whindex.rules.namedrule.dst_field", "zDestField");
        config.set("wiki.whindex.rules.namedrule.values", "zTestValue1,zTestValue2,zTestValue3,zTestValue4");
        WhindexConfig expectedConfigC = WhindexConfig.builder()
                .withValueField("zValueField")
                .withSourceField("zSourceField")
                .withOverloaded(false)
                .withDestField("zDestField")
                .withValues(Arrays.asList("zTestValue1", "zTestValue2", "zTestValue3", "zTestValue4"))
                .build();

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
    void testProcessWhindexFieldsWithDeleteSrcFieldTrue() {
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
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.processWhindexFields(eventMap);

        // Expect that the source field value is mapped to the destination field.
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("xDestField", new NormalizedFieldAndValue("xSourceField", "xTestValue1"));
        expectedValues.put("xValueField", new NormalizedFieldAndValue("xValueField", "xTestValue1"));
        Assertions.assertEquals(HashMultiset.create(expectedValues.entries()), HashMultiset.create(actualValues.entries()));
    }

    /**
     * Test that getWhindexFields() correctly transforms the event map when the rule's delete_src_field is specified as "false".
     */
    @Test
    void testProcessWhindexFieldsWithDeleteSrcFieldFalse() {
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
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.processWhindexFields(eventMap);

        // Expect that the destination field receives the transformed value.
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("yDestField", new NormalizedFieldAndValue("ySourceField", "yTestValue1"));
        expectedValues.put("yValueField", new NormalizedFieldAndValue("yValueField", "yTestValue1"));
        expectedValues.put("ySourceField", new NormalizedFieldAndValue("ySourceField", "yTestValue1"));
        Assertions.assertEquals(HashMultiset.create(expectedValues.entries()), HashMultiset.create(actualValues.entries()));
    }

    /**
     * Test that processWhindexFields() correctly handles the case when no delete_src_field property is provided.
     * <p>
     * In this case, the helper should assume delete_src_field is false.
     * </p>
     */
    @Test
    void testProcessWhindexFieldsWithDeleteSrcFieldNotPresent() {
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
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.processWhindexFields(eventMap);

        // Expect the mapping to be under the destination field since no delete_src_field is provided (assumed false).
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("zDestField", new NormalizedFieldAndValue("zSourceField", "zTestValue1"));
        expectedValues.put("zValueField", new NormalizedFieldAndValue("zValueField", "zTestValue1"));
        expectedValues.put("zSourceField", new NormalizedFieldAndValue("zSourceField", "zTestValue1"));
        Assertions.assertEquals(HashMultiset.create(expectedValues.entries()), HashMultiset.create(actualValues.entries()));
    }

    /**
     * Test that processWhindexFields() correctly handles the case when multiple whindex configs share a srcField.
     */
    @Test
    void testProcessWhindexFieldsWithOverlappingSrcFields() {

        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.o1.value_field", "oValueField");
        config.set("wiki.whindex.rules.o1.src_field", "SHARED_SRC_FIELD");
        config.set("wiki.whindex.rules.o1.dst_field", "oDestField");
        config.set("wiki.whindex.rules.o1.delete_src_field", "true"); // <-- Delete the shared srcField
        config.set("wiki.whindex.rules.o1.values", "oTestValue1,oTestValue2,oTestValue3,oTestValue4");

        config.set("wiki.whindex.rules.o2.value_field", "pValueField");
        config.set("wiki.whindex.rules.o2.src_field", "SHARED_SRC_FIELD");
        config.set("wiki.whindex.rules.o2.dst_field", "pDestField");
        config.set("wiki.whindex.rules.o2.values", "pTestValue1,pTestValue2,pTestValue3,pTestValue4");
        // Initialize the helper.
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build the event map.
        Multimap<String,NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("oValueField", new NormalizedFieldAndValue("oValueField", "oTestValue1"));
        eventMap.put("pValueField", new NormalizedFieldAndValue("pValueField", "pTestValue1"));
        eventMap.put("SHARED_SRC_FIELD", new NormalizedFieldAndValue("SHARED_SRC_FIELD", "oTestValue1,pTestValue1"));

        // Process the event map.
        Multimap<String,NormalizedContentInterface> actualValues = wHelper.processWhindexFields(eventMap);

        // Expect that both whindex fields are added despite deleting the srcField
        HashMultimap<String,NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("oDestField", new NormalizedFieldAndValue("SHARED_SRC_FIELD", "oTestValue1,pTestValue1"));
        expectedValues.put("pDestField", new NormalizedFieldAndValue("SHARED_SRC_FIELD", "oTestValue1,pTestValue1"));
        expectedValues.put("oValueField", new NormalizedFieldAndValue("oValueField", "oTestValue1"));
        expectedValues.put("pValueField", new NormalizedFieldAndValue("pValueField", "pTestValue1"));
        Assertions.assertEquals(HashMultiset.create(expectedValues.entries()), HashMultiset.create(actualValues.entries()));
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
}

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
     * Test that a configuration with no whindex rules causes the WhindexFieldIngestHelper to have an empty valueFieldsToWhindexConfigs() Multimap.
     */
    @Test
    void testEmptyWhindexConfiguration() {
        Configuration config = new Configuration();
        config.set("wiki", "");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        Multimap<String,WhindexConfig> expectedValues = LinkedListMultimap.create();

        Assertions.assertEquals(wHelper.getValueFieldsToWhindexConfigs(), expectedValues);
    }

    /**
     * Test that whindex rules are correctly parsed, even if there are several different rules configured.
     */
    @Test
    void testWhindexFieldDefinitionsParsing() {
        Configuration config = new Configuration();

        // delete src fields = true
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");
        WhindexConfig expectedConfigA = new WhindexConfig();
        expectedConfigA.setValueField("AppleVF");
        expectedConfigA.setSourceField("ForkSRC");
        expectedConfigA.setOverloaded(true);
        expectedConfigA.setDestField("ProvoloneDST");
        expectedConfigA.setValues(Arrays.asList("kittyV", "catV", "satV", "batV"));

        config.set("wiki.whindex.rules.2.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.2.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.2.values", "aV,bV,cV,dV");

        WhindexConfig expectedConfigB = new WhindexConfig();
        expectedConfigB.setValueField("aeiouVF");
        expectedConfigB.setSourceField("qwertySRC");
        expectedConfigB.setOverloaded(false);
        expectedConfigB.setDestField("poiuyDST");
        expectedConfigB.setValues(Arrays.asList("aV", "bV", "cV", "dV"));

        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        WhindexConfig expectedConfigC = new WhindexConfig();
        expectedConfigC.setValueField("hahaVF");
        expectedConfigC.setSourceField("heheSRC");
        expectedConfigC.setOverloaded(false);
        expectedConfigC.setDestField("hohohoDST");
        expectedConfigC.setValues(Arrays.asList("lolV", "lololV", "lolololV", "lololololV"));

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        HashMultimap<String,WhindexConfig> expectedValues = HashMultimap.create();
        expectedValues.put("AppleVF", expectedConfigA);
        expectedValues.put("aeiouVF", expectedConfigB);
        expectedValues.put("hahaVF", expectedConfigC);

        Assertions.assertEquals(expectedValues, wHelper.getValueFieldsToWhindexConfigs());
    }

    /**
     * Test that getWhindexFields() works when the rules specify a "true" delete_src_field.
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldTrue() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");

        // Setup helper for the "wiki" type
        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Build an event map with a corresponding entry for AppleVF and its source
        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("AppleVF", new NormalizedFieldAndValue("AppleVF", "kittyV"));
        eventMap.put("ForkSRC", new NormalizedFieldAndValue("ForkSRC", "kittyV"));

        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        // Expect the transformed value to appear under the dst_field "ProvoloneDST"
        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("ProvoloneDST", new NormalizedFieldAndValue("ForkSRC", "kittyV"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that getWhindexFields() works when the rules specify a "false" delete_src_field.
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldFalse() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.1.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.1.values", "aV,bV,cV,dV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("aeiouVF", new NormalizedFieldAndValue("aeiouVF", "aV"));
        eventMap.put("qwertySRC", new NormalizedFieldAndValue("qwertySRC", "aV"));

        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("poiuyDST", new NormalizedFieldAndValue("qwertySRC", "aV"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that getWhindexFields() works when the rules DO NOT specify delete_src_field. If no such field is provided, it is assumed to be "false".
     */
    @Test
    void testGetWhindexFieldsWithDeleteSrcFieldNotPresent() {
        Configuration config = new Configuration();
        // Note: no "delete_src_field" is set here
        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        Multimap<String, NormalizedContentInterface> eventMap = LinkedListMultimap.create();
        eventMap.put("hahaVF", new NormalizedFieldAndValue("hahaVF", "lolV"));
        eventMap.put("heheSRC", new NormalizedFieldAndValue("heheSRC", "lolV"));

        Multimap<String, NormalizedContentInterface> actualValues = wHelper.getWhindexFields(eventMap);

        HashMultimap<String, NormalizedContentInterface> expectedValues = HashMultimap.create();
        expectedValues.put("hohohoDST", new NormalizedFieldAndValue("heheSRC", "lolV"));
        Assertions.assertEquals(expectedValues, actualValues);
    }

    /**
     * Test that isWhindexField() works when the rules specify a "true" delete_src_field.
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldTrue() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Only the dst field should be considered a whindex field.
        Assertions.assertTrue(wHelper.isWhindexField("ProvoloneDST"));
       Assertions.assertFalse(wHelper.isWhindexField("ForkSRC"));
       Assertions.assertFalse(wHelper.isWhindexField("kittyV")); // value field
    }

    /**
     * Test that isWhindexField() works when the rules specify a "false" delete_src_field.
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldFalse() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.1.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.1.values", "aV,bV,cV,dV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Only the dst field should be identified as a whindex field.
       Assertions.assertTrue(wHelper.isWhindexField("poiuyDST"));
       Assertions.assertFalse(wHelper.isWhindexField("qwertySRC"));
       Assertions.assertFalse(wHelper.isWhindexField("aV")); // value
    }

    /**
     * Test that isWhindexField() works when the rules DO NOT specify delete_src_field. If no such field is provided, it is assumed to be "false".
     */
    @Test
    void testWhindexFieldIdentificationWithDeleteSrcFieldNotPresent() {
        Configuration config = new Configuration();
        // No delete_src_field property provided
        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Only the destination field should be seen as a whindex field.
       Assertions.assertTrue(wHelper.isWhindexField("hohohoDST"));
       Assertions.assertFalse(wHelper.isWhindexField("heheSRC"));
       Assertions.assertFalse(wHelper.isWhindexField("lolV")); // value
    }

    /**
     * Test that isOverloadedWhindexField() works when the rules specify a "true" delete_src_field.
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldTrue() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // Only the src field from the rule with delete_src_field true is overloaded.
       Assertions.assertTrue(wHelper.isOverloadedWhindexField("ForkSRC"));
        // Also check that the dst field is not overloaded.
       Assertions.assertFalse(wHelper.isOverloadedWhindexField("ProvoloneDST"));
    }

    /**
     * Test that isOverloadedWhindexField() works when the rules specify a "false" delete_src_field.
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldFalse() {
        Configuration config = new Configuration();
        config.set("wiki.whindex.rules.1.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.1.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "false");
        config.set("wiki.whindex.rules.1.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.1.values", "aV,bV,cV,dV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // When delete_src_field is false the source field should not be seen as overloaded.
       Assertions.assertFalse(wHelper.isOverloadedWhindexField("qwertySRC"));
       Assertions.assertFalse(wHelper.isOverloadedWhindexField("poiuyDST")); // dst field is not overloaded either
    }

    /**
     * Test that isOverloadedWhindexField() works when the rules DO NOT specify delete_src_field. If no such field is provided, it is assumed to be "false".
     */
    @Test
    void testOverloadedFieldIdentificationWithDeleteSrcFieldNotPresent() {
        Configuration config = new Configuration();
        // No delete_src_field property provided
        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // With no delete_src_field property, the source field is not overloaded.
       Assertions.assertFalse(wHelper.isOverloadedWhindexField("heheSRC"));
       Assertions.assertFalse(wHelper.isOverloadedWhindexField("hohohoDST"));
    }
    
}

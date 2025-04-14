package datawave.ingest.data.config.ingest;

import java.util.Arrays;

import com.google.common.collect.HashMultimap;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;

class WhindexFieldIngestHelperTest {

    /*
     * Tests that a configuration without a <type>.rules property does not have whindex field definitions.
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

    /*
     * Test that whindex fields and their values are correctly identified by and added to the WhindexFieldIngestHelper.
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
        expectedConfigA.setValues(Arrays.asList("kittyV","catV","satV","batV"));


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

    // /*
    // * WIP - I think I did this wrong. todo: this only matters when we have a concrete idea of what gwf() actually does.
    // */
    // @Test
    // void testGetWhindexFields() {
    // Configuration config = new Configuration();
    // config.set("wiki.rules", "srcA:dstA:true,srcB:dstB::v1,v2;srcC:dstC:false::v3,v4");
    //
    // Type type = new Type("wiki", null, null, null, 0, null);
    // WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
    // wHelper.setup(config);
    //
    // Multimap<String,NormalizedContentInterface> paramMap = LinkedListMultimap.create();
    // paramMap.put("dstA", new NormalizedFieldAndValue("dstA", "v1,v2"));
    // paramMap.put("dstB", new NormalizedFieldAndValue("dstB", "v1,v2"));
    // paramMap.put("dstC", new NormalizedFieldAndValue("dstC", "v3,v4"));
    // paramMap.put("fakeA", new NormalizedFieldAndValue("fakeA", "v3,v4"));
    // paramMap.put("fakeB", new NormalizedFieldAndValue("fakeB", "v5"));
    // paramMap.put("fakeC", new NormalizedFieldAndValue("fakeC", "v5,v6"));
    // paramMap.put("fakeD", new NormalizedFieldAndValue("fakeD", "v1,v7"));
    //
    // Multimap<String,NormalizedContentInterface> actualValues = wHelper.getWhindexFields(paramMap);
    //
    // Multimap<String,NormalizedContentInterface> expectedValues = LinkedListMultimap.create();
    // expectedValues.put("dstA", new NormalizedFieldAndValue("dstA", "v1,v2"));
    // expectedValues.put("dstB", new NormalizedFieldAndValue("dstB", "v1,v2"));
    // expectedValues.put("dstC", new NormalizedFieldAndValue("dstC", "v3,v4"));
    //
    // Assertions.assertEquals(expectedValues, actualValues);
    // }

    /*
     * Tests that the WhindexFieldIngestHelper correctly identified whindex fields via isWhindexField() after setup.
     */
    @Test
    void testWhindexFieldIdentification() {

        Configuration config = new Configuration();

        // delete src fields = true
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");

        // delete src fields = false
        config.set("wiki.whindex.rules.2.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.2.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.2.values", "aV,bV,cV,dV");

        // delete src fields = empty
        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // check dst fields are interpreted as whindex fields
        Assertions.assertTrue(wHelper.isWhindexField("ProvoloneDST"));
        Assertions.assertTrue(wHelper.isWhindexField("poiuyDST"));
        Assertions.assertTrue(wHelper.isWhindexField("hohohoDST"));

        // check src fields are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("ForkSRC"));
        Assertions.assertFalse(wHelper.isWhindexField("qwertySRC"));
        Assertions.assertFalse(wHelper.isWhindexField("heheSRC"));

        // check values are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("kittyV"));
        Assertions.assertFalse(wHelper.isWhindexField("aV"));
        Assertions.assertFalse(wHelper.isWhindexField("lolV"));
        Assertions.assertFalse(wHelper.isWhindexField("lololV"));

        // check other fields are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("asdfghjkl"));
        Assertions.assertFalse(wHelper.isWhindexField("true"));
        Assertions.assertFalse(wHelper.isWhindexField("{true}"));
        Assertions.assertFalse(wHelper.isWhindexField("false"));
        Assertions.assertFalse(wHelper.isWhindexField("{false}"));
    }

    /*
     * Tests that WhindexFieldIngestHelper correctly identifies overloaded fields after setup.
     */
    @Test
    void testOverloadedFieldIdentification() {

        Configuration config = new Configuration();

        // delete src fields = true
        config.set("wiki.whindex.rules.1.value_field", "AppleVF");
        config.set("wiki.whindex.rules.1.src_field", "ForkSRC");
        config.set("wiki.whindex.rules.1.delete_src_field", "true");
        config.set("wiki.whindex.rules.1.dst_field", "ProvoloneDST");
        config.set("wiki.whindex.rules.1.values", "kittyV,catV,satV,batV");

        // delete src fields = false
        config.set("wiki.whindex.rules.2.value_field", "aeiouVF");
        config.set("wiki.whindex.rules.2.src_field", "qwertySRC");
        config.set("wiki.whindex.rules.2.delete_src_field", "false");
        config.set("wiki.whindex.rules.2.dst_field", "poiuyDST");
        config.set("wiki.whindex.rules.2.values", "aV,bV,cV,dV");

        // delete src fields = empty
        config.set("wiki.whindex.rules.sillyrule.value_field", "hahaVF");
        config.set("wiki.whindex.rules.sillyrule.src_field", "heheSRC");
        config.set("wiki.whindex.rules.sillyrule.dst_field", "hohohoDST");
        config.set("wiki.whindex.rules.sillyrule.values", "lolV,lololV,lolololV,lololololV");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // check dst fields are interpreted as whindex fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("ProvoloneDST"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("poiuyDST"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("hohohoDST"));

        // check src fields are not interpreted as whindex fields
        Assertions.assertTrue(wHelper.isOverloadedWhindexField("ForkSRC"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("qwertySRC"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("heheSRC"));

        // check values are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("kittyV"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("aV"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("lolV"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("lololV"));

        // check other fields are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("asdfghjkl"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("true"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("{true}"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("false"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("{false}"));
    }
}

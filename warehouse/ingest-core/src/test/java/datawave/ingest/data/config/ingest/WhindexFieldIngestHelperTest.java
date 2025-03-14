package datawave.ingest.data.config.ingest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

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

        Multimap<String,String> expectedValues = LinkedListMultimap.create();

        Assertions.assertEquals(wHelper.getWhindexFieldDefinitions(), expectedValues);
    }

    /*
     * Test that whindex fields and their values are correctly identified by and added to the WhindexFieldIngestHelper.
     */
    @Test
    void testWhindexFieldDefinitionsParsing() {
        Configuration config = new Configuration();
        config.set("wiki.rules", "srcA:dstA:true,srcB:dstB::v1,v2;srcC:dstC:false::v3,v4");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        Multimap<String,String> expectedValues = LinkedListMultimap.create();
        expectedValues.putAll("dstA", Arrays.asList("v1", "v2"));
        expectedValues.putAll("dstB", Arrays.asList("v1", "v2"));
        expectedValues.putAll("dstC", Arrays.asList("v3", "v4"));

        Assertions.assertEquals(expectedValues, wHelper.getWhindexFieldDefinitions());
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
        config.set("wiki.rules", "srcA:dstA:true,srcB:dstB::v1,v2;srcC:dstC:false::v3,v4");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // check dst fields are interpreted as whindex fields
        Assertions.assertTrue(wHelper.isWhindexField("dstA"));
        Assertions.assertTrue(wHelper.isWhindexField("dstB"));
        Assertions.assertTrue(wHelper.isWhindexField("dstC"));

        // check src fields are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("srcA"));
        Assertions.assertFalse(wHelper.isWhindexField("srcB"));
        Assertions.assertFalse(wHelper.isWhindexField("srcC"));

        // check values are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("v1"));
        Assertions.assertFalse(wHelper.isWhindexField("v2"));
        Assertions.assertFalse(wHelper.isWhindexField("v3"));
        Assertions.assertFalse(wHelper.isWhindexField("v4"));

        // check other fields are not interpreted as whindex fields
        Assertions.assertFalse(wHelper.isWhindexField("Apple"));
        Assertions.assertFalse(wHelper.isWhindexField("true"));
        Assertions.assertFalse(wHelper.isWhindexField("{true}"));
    }

    /*
     * Tests that WhindexFieldIngestHelper correctly identifies overloaded fields after setup.
     */
    @Test
    void testOverloadedFieldIdentification() {

        Configuration config = new Configuration();
        config.set("wiki.rules", "srcA:dstA:true,srcB:dstB::v1,v2;srcC:dstC:false::v3,v4");

        Type type = new Type("wiki", null, null, null, 0, null);
        WhindexFieldIngestHelper wHelper = new WhindexFieldIngestHelper(type);
        wHelper.setup(config);

        // check dst fields are not interpreted as overloaded fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("dstA"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("dstB"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("dstC"));

        // check src fields are/aren't interpreted as overloaded fields
        Assertions.assertTrue(wHelper.isOverloadedWhindexField("srcA"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("srcB"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("srcC"));

        // check values are not interpreted as overloaded fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("v1"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("v2"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("v3"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("v4"));

        // check other fields are not interpreted as overloaded fields
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("Apple"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("true"));
        Assertions.assertFalse(wHelper.isOverloadedWhindexField("{true}"));
    }
}

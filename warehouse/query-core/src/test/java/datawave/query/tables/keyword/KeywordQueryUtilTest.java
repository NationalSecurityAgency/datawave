package datawave.query.tables.keyword;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.NoOpType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.TypeAttribute;

public class KeywordQueryUtilTest {

    Attribute<?> typeAttributeOne;
    Attribute<?> typeAttributeTwo;
    Attribute<?> contentAttributeOne;
    Attribute<?> contentAttributeTwo;
    Attribute<?> attributesOne;
    Attribute<?> attributesTwo;
    Attribute<?> attributesHetOne;
    Attribute<?> attributesHetTwo;

    @BeforeEach
    public void setupTestData() {
        Key docKey = new Key("shard", "datatype\0uid");

        typeAttributeOne = new TypeAttribute<>(new NoOpType("VALUE_ONE"), docKey, true);
        typeAttributeTwo = new TypeAttribute<>(new NoOpType("VALUE_TWO"), docKey, true);
        contentAttributeOne = new Content("content attribute one", docKey, true);
        contentAttributeTwo = new Content("content attribute two", docKey, true);

        attributesOne = new Attributes(List.of(typeAttributeOne, typeAttributeTwo), true);
        attributesTwo = new Attributes(List.of(contentAttributeOne, contentAttributeTwo), true);

        attributesHetOne = new Attributes(List.of(contentAttributeOne, attributesOne, contentAttributeTwo), true);
        attributesHetTwo = new Attributes(List.of(attributesTwo, typeAttributeOne, contentAttributeTwo), true);
    }

    @Test
    public void testGetSingleValuesFromAttribute() {
        assertSingleValue("VALUE_ONE", KeywordQueryUtil.getStringValuesFromAttribute(typeAttributeOne));
        assertSingleValue("VALUE_TWO", KeywordQueryUtil.getStringValuesFromAttribute(typeAttributeTwo));
        assertSingleValue("content attribute one", KeywordQueryUtil.getStringValuesFromAttribute(contentAttributeOne));
        assertSingleValue("content attribute two", KeywordQueryUtil.getStringValuesFromAttribute(contentAttributeTwo));
    }

    @Test
    public void testGetMultipleValuesFromAttributes() {
        assertMultipleValues(List.of("VALUE_ONE", "VALUE_TWO"), KeywordQueryUtil.getStringValuesFromAttribute(attributesOne));
        assertMultipleValues(List.of("content attribute one", "content attribute two"), KeywordQueryUtil.getStringValuesFromAttribute(attributesTwo));
    }

    @Test
    public void testGetMultipleValuesFromHetAttributes() {
        assertMultipleValues(List.of("VALUE_ONE", "VALUE_TWO", "content attribute one", "content attribute two"),
                        KeywordQueryUtil.getStringValuesFromAttribute(attributesHetOne));
        assertMultipleValues(List.of("VALUE_ONE", "content attribute one", "content attribute two", "content attribute two"),
                        KeywordQueryUtil.getStringValuesFromAttribute(attributesHetTwo));
    }

    public static void assertSingleValue(String expectedValue, List<String> results) {
        assertNotNull(results, "results should not have been null");
        assertFalse(results.isEmpty(), "results should not have been empty");
        assertEquals(1, results.size(), "results should have been size 1");
        assertEquals(expectedValue, results.get(0), "results did not contain the expected value: " + expectedValue);
    }

    public static void assertMultipleValues(List<String> expectedValues, List<String> results) {
        assertNotNull(results, "results should not have been null");
        assertFalse(results.isEmpty(), "results should not have been empty");
        assertEquals(expectedValues.size(), results.size(), "results did not have the expected size: " + results);
        final List<String> unseen = new ArrayList<>(expectedValues);
        for (String seen : results) {
            unseen.remove(seen);
        }
        assertTrue(unseen.isEmpty(), "results did not contain the value: " + unseen);
    }
}

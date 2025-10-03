package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.util.TypeMetadata;

public class LimitFieldsTest {

    private final Key key = new Key("20250202_0", "datatype\0uid");

    private AttributeFactory attributeFactory;
    private PreNormalizedAttributeFactory preNormalizedAttributeFactory;
    private TypeMetadata typeMetadata;

    private Map<String,Integer> limitMap;
    private Document document;
    private Document result;

    @BeforeEach
    public void setup() {
        limitMap = new HashMap<>();
        document = new Document();
        result = null;
    }

    @Test
    public void testSingleEventFieldNoLimit() {
        createEvent("FIELD_A", "value-a", true);
        drive();
        assertFieldCount("FIELD_A", 1);
    }

    @Test
    public void testSingleIndexFieldNoLimit() {
        createIndex("FIELD_A", "value-a", true);
        drive();
        assertFieldCount("FIELD_A", 1);
    }

    @Test
    public void testMergedFieldsNoLimit() {
        createEvent("FIELD_A", "value-a", true);
        createIndex("FIELD_A", "value-a", true);
        drive();
        assertFieldCount("FIELD_A", 2);
    }

    @Test
    public void testLimitEventFields() {
        createEvent("FIELD_A", "value-a", true);
        createEvent("FIELD_A", "value-b", true);
        withLimit("FIELD_A", 1);
        drive();
        assertFieldCount("FIELD_A", 1);
        assertOriginalCount("FIELD_A", 2);
    }

    @Test
    public void testLimitIndexFields() {
        createIndex("FIELD_A", "value-a", true);
        createIndex("FIELD_A", "value-b", true);
        withLimit("FIELD_A", 1);
        drive();
        assertFieldCount("FIELD_A", 1);
        assertOriginalCount("FIELD_A", 2);
    }

    @Test
    public void testLimitMergedFields() {
        createEvent("FIELD_A", "value-a", true);
        createIndex("FIELD_A", "value-b", true);
        withLimit("FIELD_A", 1);
        drive();
        assertFieldCount("FIELD_A", 1);
        assertOriginalCount("FIELD_A", 2);
    }

    @Test
    public void testDoNotLimitEventFieldsThatAreAlsoHitTerms() {
        createEvent("FIELD_A", "value-a", true);
        createEvent("FIELD_A", "value-b", true);
        withLimit("FIELD_A", 1);
        withHitTerm("FIELD_A", "value-a");
        withHitTerm("FIELD_A", "value-b");
        drive();
        assertFieldCount("FIELD_A", 2);
        assertNoOriginalCount("FIELD_A");
    }

    @Test
    public void testDoNotLimitIndexFieldsThatAreAlsoHitTerms() {
        createIndex("FIELD_A", "value-a", true);
        createIndex("FIELD_A", "value-b", true);
        withLimit("FIELD_A", 1);
        withHitTerm("FIELD_A", "value-a");
        withHitTerm("FIELD_A", "value-b");
        drive();
        assertFieldCount("FIELD_A", 2);
        assertNoOriginalCount("FIELD_A");
    }

    @Test
    public void testDoNotLimitMergedFieldsThatAreAlsoHitTerms() {
        createEvent("FIELD_A", "value-a", true);
        createIndex("FIELD_A", "value-a", true);
        withLimit("FIELD_A", 1);
        withHitTerm("FIELD_A", "value-a");
        drive();
        assertFieldCount("FIELD_A", 2);
        assertNoOriginalCount("FIELD_A");
    }

    private void drive() {
        LimitFields limitFields = new LimitFields(limitMap, null);
        Map.Entry<Key,Document> input = new AbstractMap.SimpleEntry<>(key, document);
        Map.Entry<Key,Document> applied = limitFields.apply(input);
        result = applied.getValue();
    }

    private void assertFieldCount(String field, int count) {
        assertTrue(result.containsKey(field));
        assertEquals(count, result.getDictionary().get(field).size());
    }

    private void assertOriginalCount(String field, int count) {
        String key = field + LimitFields.ORIGINAL_COUNT_SUFFIX;
        assertTrue(result.containsKey(key));
        Attribute<?> attr = result.getDictionary().get(key);
        assertInstanceOf(Numeric.class, attr);
        Numeric numeric = (Numeric) attr;
        assertEquals(count, numeric.getData());
    }

    private void assertNoOriginalCount(String field) {
        String key = field + LimitFields.ORIGINAL_COUNT_SUFFIX;
        assertFalse(result.containsKey(key));
    }

    private void withLimit(String field, int value) {
        limitMap.put(field, value);
    }

    private void withHitTerm(String field, String value) {
        Attribute<?> source = getAttributeFactory().create(field, value, key, true);
        document.put("HIT_TERM", new Content(field + ":" + value, key, true, source));
    }

    private void createEvent(String field, String value) {
        createEvent(field, value, true);
    }

    private void createEvent(String field, String value, boolean toKeep) {
        document.put(field, getAttributeFactory().create(field, value, key, toKeep));
    }

    private void createIndex(String field, String value) {
        createIndex(field, value, true);
    }

    private void createIndex(String field, String value, boolean toKeep) {
        document.put(field, getPreNormalizedAttributeFactory().create(field, value, key, toKeep));
    }

    private AttributeFactory getAttributeFactory() {
        if (attributeFactory == null) {
            attributeFactory = new AttributeFactory(getTypeMetadata());
        }
        return attributeFactory;
    }

    private PreNormalizedAttributeFactory getPreNormalizedAttributeFactory() {
        if (preNormalizedAttributeFactory == null) {
            preNormalizedAttributeFactory = new PreNormalizedAttributeFactory(getTypeMetadata());
        }
        return preNormalizedAttributeFactory;
    }

    private TypeMetadata getTypeMetadata() {
        if (typeMetadata == null) {
            typeMetadata = new TypeMetadata();
        }
        return typeMetadata;
    }
}

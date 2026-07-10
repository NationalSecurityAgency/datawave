package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    /**
     * Two hit terms may share the same value while remaining distinct attributes, e.g. when the value was matched in both its event form (TypeAttribute) and
     * its index form (PreNormalizedAttribute). Both attributes must be recognized as hits.
     */
    @Test
    public void testDoNotLimitHitTermsWithSameValueFromEventAndIndexSources() {
        createEvent("FIELD_A", "value-a", true);
        createIndex("FIELD_B", "value-a", true);
        withLimit("FIELD_A", -1);
        withLimit("FIELD_B", -1);
        withHitTerm("FIELD_A", "value-a");
        withIndexHitTerm("FIELD_B", "value-a");
        drive();
        assertFieldCount("FIELD_A", 1);
        assertFieldCount("FIELD_B", 1);
        assertNoOriginalCount("FIELD_A");
        assertNoOriginalCount("FIELD_B");
    }

    /**
     * Two hit terms may share the same value under different metadata, e.g. different column visibilities within the same document. Each attribute must be
     * matched against the hit-term key it belongs to, so both must be kept.
     */
    @Test
    public void testDoNotLimitHitTermsWithSameValueUnderDifferentVisibilities() {
        Key keyVisA = new Key("20250202_0", "datatype\0uid", "", "VIS_A", 0L);
        Key keyVisB = new Key("20250202_0", "datatype\0uid", "", "VIS_B", 0L);
        createEvent("FIELD_A", "value-a", keyVisA);
        createEvent("FIELD_B", "value-a", keyVisB);
        withLimit("FIELD_A", -1);
        withLimit("FIELD_B", -1);
        withHitTerm("FIELD_A", "value-a", keyVisA);
        withHitTerm("FIELD_B", "value-a", keyVisB);
        drive();
        assertFieldCount("FIELD_A", 1);
        assertFieldCount("FIELD_B", 1);
        assertNoOriginalCount("FIELD_A");
        assertNoOriginalCount("FIELD_B");
    }

    @Test
    public void testContextBuild() {
        Key docKey = new Key("shard", "datatype\0uid");
        Content attr1 = new Content("a", docKey, true);
        Content attr2 = new Content("b", docKey, true);
        Content attr3 = new Content("c", docKey, true);
        Content attr4 = new Content("d", docKey, true);

        // @formatter:off
        LimitFields.HitTermContext context = new LimitFields.HitTermContext.Builder()
            .putHitField("FIELD_1.FIELD.5.3", attr1)
            .putHitField("FIELD_1.FIELD.5.3", attr2)
            .putHitField("FIELD_2.FIELD.5.3", attr3)
            .putHitField("VAL_2.BAR.6.3", attr4)
            .build();
        // @formatter:on

        assertEquals(2, context.getGroupAndInstanceSet().size());

        assertTrue(context.containsFieldWithGrouping("FIELD_1.FIELD.5.3"));
        assertTrue(context.hasGroupAndInstance(FieldName.parse("FOO_3.FIELD.7.3").getGroupAndInstance()));
        assertTrue(context.hasGroupAndInstance(FieldName.parse("VAL_2.BAR.6.3").getGroupAndInstance()));
        assertTrue(context.hasGroupAndInstance(FieldName.parse("VAL_2.BAR.7.3").getGroupAndInstance()));
        assertTrue(context.hasGroupAndInstance(FieldName.parse("VAL_1.BAR.7.3").getGroupAndInstance()));
        assertEquals(Set.of(attr1, attr2, attr3, attr4), Set.copyOf(context.getHitTermAttributes()));
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
        withHitTerm(field, value, key);
    }

    private void withHitTerm(String field, String value, Key key) {
        Attribute<?> source = getAttributeFactory().create(field, value, key, true);
        document.put("HIT_TERM", new Content(field + ":" + value, key, true, source));
    }

    private void withIndexHitTerm(String field, String value) {
        Attribute<?> source = getPreNormalizedAttributeFactory().create(field, value, key, true);
        document.put("HIT_TERM", new Content(field + ":" + value, key, true, source));
    }

    private void createEvent(String field, String value) {
        createEvent(field, value, true);
    }

    private void createEvent(String field, String value, boolean toKeep) {
        createEvent(field, value, key, toKeep);
    }

    private void createEvent(String field, String value, Key key) {
        createEvent(field, value, key, true);
    }

    private void createEvent(String field, String value, Key key, boolean toKeep) {
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

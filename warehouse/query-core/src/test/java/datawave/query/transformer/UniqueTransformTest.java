package datawave.query.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.DiacriticContent;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.attributes.UniqueFields;
import datawave.query.attributes.UniqueGranularity;
import datawave.query.function.LogTiming;
import datawave.query.jexl.JexlASTHelper;

public class UniqueTransformTest {

    private static final Random random = new Random(1000);
    private static final List<String> randomValues = new ArrayList<>();

    private final List<Document> inputDocuments = new ArrayList<>();
    private final List<Document> expectedUniqueDocuments = new ArrayList<>();
    private byte[] expectedOrderedFieldValues = null;
    private UniqueFields uniqueFields = new UniqueFields();

    @BeforeClass
    public static void setup() {
        for (int i = 0; i < 5; i++) {
            int length = random.nextInt(11) + 10;
            randomValues.add(RandomStringUtils.randomAlphanumeric(length));
        }
    }

    @After
    public void tearDown() throws Exception {
        inputDocuments.clear();
        expectedUniqueDocuments.clear();
        uniqueFields = new UniqueFields();
        expectedOrderedFieldValues = null;
    }

    @Test
    public void testTransformingNullReturnsNull() {
        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0");

        UniqueTransform uniqueTransform = getUniqueTransform();

        assertNull(uniqueTransform.apply(null));
    }

    @Test
    public void testUniquenessWithRandomDocuments() {
        // Create 100 random documents.
        for (int i = 0; i < 100; i++) {
            givenInputDocument().withRandomKeyValues(10, 100, 50);
        }

        // Choose three fields such that the number of unique document is less than half the number of documents but greater than 10.
        Set<String> fields = new HashSet<>();
        int expectedUniqueDocuments = inputDocuments.size();
        while (expectedUniqueDocuments > inputDocuments.size() / 2 || expectedUniqueDocuments < 10) {
            fields.clear();
            while (fields.size() < 3) {
                fields.add("Attr" + random.nextInt(100));
            }
            expectedUniqueDocuments = countUniqueness(inputDocuments, fields);
        }

        givenValueTransformerForFields(UniqueGranularity.ALL, fields.toArray(new String[0]));

        List<Document> uniqueDocuments = getUniqueDocuments(inputDocuments);
        assertEquals(expectedUniqueDocuments, uniqueDocuments.size());
    }

    private int countUniqueness(List<Document> input, Set<String> fields) {
        Set<String> uniqueValues = new HashSet<>();
        for (Document document : input) {
            Multimap<String,String> fieldValues = getFieldValues(document, fields);
            uniqueValues.add(getString(fieldValues));
        }
        return uniqueValues.size();
    }

    private Multimap<String,String> getFieldValues(Document document, Set<String> fields) {
        Multimap<String,String> values = HashMultimap.create();
        for (String docField : document.getDictionary().keySet()) {
            for (String field : fields) {
                if (docField.equalsIgnoreCase(field)) {
                    Attribute<?> attribute = document.get(docField);
                    if (attribute instanceof Attributes) {
                        ((Attributes) attribute).getAttributes().stream().map(Attribute::getData).map(String::valueOf).forEach((val) -> values.put(field, val));
                    } else {
                        values.put(field, String.valueOf(attribute.getData()));
                    }
                }
            }
        }
        return values;
    }

    private String getString(Multimap<String,String> fieldValues) {
        StringBuilder sb = new StringBuilder();
        fieldValues.keySet().stream().sorted().forEach((field) -> {
            if (sb.length() > 0) {
                sb.append("/ ");
            }
            sb.append(field).append(":");
            sb.append(fieldValues.get(field).stream().sorted().collect(Collectors.joining(",")));
        });
        return sb.toString();
    }

    /**
     * Verify that field matching is case-insensitive. Query: #UNIQUE(attr0, Attr1, ATTR2)
     */
    @Test
    public void testUniquenessForCaseInsensitivity() {
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(1)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(0));
        givenInputDocument().withKeyValue("Attr1", randomValues.get(2)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr1", randomValues.get(3)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr1", randomValues.get(2));
        givenInputDocument().withKeyValue("attr2", randomValues.get(4)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("attr2", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("attr2", randomValues.get(4));

        givenValueTransformerForFields(UniqueGranularity.ALL, "attr0", "Attr1", "ATTR2");

        assertUniqueDocuments();
    }

    /**
     * Verify the DAY function will truncate date values to their day and determine uniqueness based on that when possible. Query: #UNIQUE(#DAY(Attr0))
     */
    @Test
    public void testUniquenessWithValueTransformer_DAY() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 12:40:15");
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 05:04:20");
        givenInputDocument().withKeyValue("Attr0", "2001-03-12 05:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY, "Attr0");

        assertUniqueDocuments();
    }

    /**
     * Verify the HOUR function will truncate date values to their hour and determine uniqueness based on that when possible. Query: #UNIQUE(#HOUR(Attr0))
     */
    @Test
    public void testUniquenessWithValueTransformer_HOUR() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:40:15");
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 05:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 05:04:30");
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR, "Attr0");

        assertUniqueDocuments();
    }

    /**
     * Verify the MINUTE function will truncate date values to their minute and determine uniqueness based on that when possible. Query: #UNIQUE(#MINUTE(Attr0))
     */
    @Test
    public void testUniquenessWithValueTransformer_MINUTE() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:20");
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:04:15");
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE, "Attr0");

        assertUniqueDocuments();
    }

    /**
     * Verify mixed value transformers for different fields applies the transformers only to relevant fields. Query: #UNIQUE(#DAY(Attr0)) AND
     * #UNIQUE(#HOUR(Attr1)) and #UNIQUE(#MINUTE(Attr2))
     */
    @Test
    public void testUniquenessWithMixedValueTransformersForDifferentFields() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 12:40:15");
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 05:04:20");
        givenInputDocument().withKeyValue("Attr0", "2001-03-12 05:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr1", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr1", "2001-03-10 10:40:15");
        givenInputDocument().withKeyValue("Attr1", "2001-03-10 05:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr1", "2001-03-10 05:04:30");
        givenInputDocument().withKeyValue("Attr2", "2001-03-10 10:15:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr2", "2001-03-10 10:15:20");
        givenInputDocument().withKeyValue("Attr2", "2001-03-10 10:04:20").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr2", "2001-03-10 10:04:15");

        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY, "Attr0");
        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR, "Attr1");
        givenValueTransformerForFields(UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE, "Attr2");

        assertUniqueDocuments();
    }

    /**
     * Verify that the ALL function finds more unique documents than MINUTE when they are provided for the same field. Query: #UNIQUE(Attr0) AND
     * #UNIQUE(#MINUTE(Attr0))
     */
    @Test
    public void testThatValueTransformer_ALL_Supersedes_MINUTE() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:01").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:02").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:03").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:04").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:15:04");
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformersForField("Attr0", UniqueGranularity.ALL, UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);

        assertUniqueDocuments();
    }

    /**
     * Verify that the MINUTE function finds more unique documents than HOUR when they are provided for the same field. Query: #UNIQUE(#MINUTE(Attr0)) AND
     * #UNIQUE(#HOUR(Attr0))
     */
    @Test
    public void testThatValueTransformer_MINUTE_Supersedes_HOUR() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:01:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:02:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:03:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:04:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:04:20");
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformersForField("Attr0", UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE, UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);

        assertUniqueDocuments();
    }

    /**
     * Verify that the HOUR function finds more unique documents than DAY when they are provided for the same field. Query: #UNIQUE(#HOUR(Attr0)) AND
     * #UNIQUE(#DAY(Attr0))
     */
    @Test
    public void testThatValueTransformer_HOUR_Supersedes_DAY() {
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 10:01:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 11:01:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 12:01:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 13:01:15").isExpectedToBeUnique();
        givenInputDocument().withKeyValue("Attr0", "2001-03-10 13:20:15");
        givenInputDocument().withKeyValue("Attr0", "nonDateValue").isExpectedToBeUnique();

        givenValueTransformersForField("Attr0", UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR, UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);

        assertUniqueDocuments();
    }

    @Test
    public void testUniquenessWithTimingMetric() {
        List<Document> input = new ArrayList<>();
        List<Document> expected = new ArrayList<>();

        String MARKER_STRING = "\u2735FinalDocument\u2735";
        TimingMetadata timingMetadata = new TimingMetadata();
        timingMetadata.setNextCount(5l);

        givenInputDocument(MARKER_STRING).withKeyValue(LogTiming.TIMING_METADATA, timingMetadata.toString()).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR1", randomValues.get(1)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR1", randomValues.get(2));

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0");

        assertUniqueDocuments();
    }

    /**
     * Test that groups get placed into separate field sets
     */
    @Test
    public void testUniquenessWithTwoGroups() {
        // Create document with two fields as follows:
        // field1.group1
        // field2.group1
        // field1.group2
        // field2.group2

        // @formatter:off
        givenInputDocument()
                .withKeyValue("Attr0.0.0.0", randomValues.get(0))
                .withKeyValue("Attr1.0.1.0", randomValues.get(1))
                .withKeyValue("Attr0.0.0.1", randomValues.get(2))
                .withKeyValue("Attr1.0.1.1", randomValues.get(3));

        expectedOrderedFieldValues = givenExpectedOrderedFieldValues()
                .withKeyValue("Attr0", randomValues.get(0))
                .withKeyValue("Attr1", randomValues.get(1))
                .withKeyValue("Attr0", randomValues.get(2))
                .withKeyValue("Attr1", randomValues.get(3)).build();
        // @formatter:on

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0", "Attr1");

        assertOrderedFieldValues();
    }

    /**
     * Test that groups get placed into separate field sets combined with ungrouped attributes
     */
    @Test
    public void testUniquenessWithTwoGroupsAndUngrouped() {
        // Create document with two fields as follows:
        // field1.group1
        // field1.group2
        // field2.group1
        // field2.group2
        // field3

        // @formatter:off
        givenInputDocument()
                .withKeyValue("Attr0.0.0.0", randomValues.get(0))
                .withKeyValue("Attr1.0.1.0", randomValues.get(1))
                .withKeyValue("Attr0.0.0.1", randomValues.get(2))
                .withKeyValue("Attr1.0.1.1", randomValues.get(3))
                .withKeyValue("Attr3", randomValues.get(4));

        expectedOrderedFieldValues = givenExpectedOrderedFieldValues()
                .withKeyValue("Attr0", randomValues.get(0))
                .withKeyValue("Attr1", randomValues.get(1))
                .withKeyValue("Attr0", randomValues.get(2))
                .withKeyValue("Attr1", randomValues.get(3))
                .withKeyValue("Attr3", randomValues.get(4)).build();
        // @formatter:on

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0", "Attr1", "Attr3");

        assertOrderedFieldValues();
    }

    /**
     * Test that groups get placed into separate field sets combined with a separately grouped attributes
     */
    @Test
    public void testUniquenessWithTwoGroupsAndSeparateGroup() {
        // create document two fields as follows:
        // field1.group1
        // field1.group2
        // field2.group1
        // field2.group2
        // field3.group3

        // @formatter:off
        givenInputDocument()
                .withKeyValue("Attr0.0.0.0", randomValues.get(0))
                .withKeyValue("Attr1.0.1.0", randomValues.get(1))
                .withKeyValue("Attr0.0.0.1", randomValues.get(2))
                .withKeyValue("Attr1.0.1.1", randomValues.get(3))
                .withKeyValue("Attr3.1.0.0", randomValues.get(4));

        expectedOrderedFieldValues = givenExpectedOrderedFieldValues()
                .withKeyValue("Attr0", randomValues.get(0))
                .withKeyValue("Attr1", randomValues.get(1))
                .withKeyValue("Attr0", randomValues.get(2))
                .withKeyValue("Attr1", randomValues.get(3))
                .withKeyValue("Attr3", randomValues.get(4)).build();
        // @formatter:on

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0", "Attr1", "Attr3");

        assertOrderedFieldValues();
    }

    /**
     * Test that groups get placed into separate field sets combined with a separately grouped attributes
     */
    @Test
    public void testUniquenessWithTwoGroupsAndSeparateGroups() {
        // create document two fields as follows:
        // field1.group1
        // field1.group2
        // field2.group1
        // field2.group2
        // field3.group3
        // field3.group4

        // @formatter:off
        givenInputDocument()
                .withKeyValue("Attr0.0.0.0", randomValues.get(0))
                .withKeyValue("Attr1.0.1.0", randomValues.get(1))
                .withKeyValue("Attr0.0.0.1", randomValues.get(2))
                .withKeyValue("Attr1.0.1.1", randomValues.get(3))
                .withKeyValue("Attr3.1.0.0", randomValues.get(4))
                .withKeyValue("Attr3.1.0.1", randomValues.get(0));

        expectedOrderedFieldValues = givenExpectedOrderedFieldValues()
                .withKeyValue("Attr0", randomValues.get(0))
                .withKeyValue("Attr1", randomValues.get(1))
                .withKeyValue("Attr0", randomValues.get(2))
                .withKeyValue("Attr1", randomValues.get(3))
                .withKeyValue("Attr3", randomValues.get(4))
                .withKeyValue("Attr3", randomValues.get(0)).build();
        // @formatter:on

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0", "Attr1", "Attr3");

        assertOrderedFieldValues();
    }

    /**
     * Test that groups get placed into separate field sets combined with a separately grouped attributes
     */
    @Test
    public void testUniquenessWithTwoGroupsAndPartialGroups() {
        // create document two fields as follows:
        // field1.group1
        // field1.group2
        // field2.group1 (note no field2.group2 created)
        // field3.group3

        // @formatter:off
        givenInputDocument()
                .withKeyValue("Attr0.0.0.0", randomValues.get(0))
                .withKeyValue("Attr1.0.1.0", randomValues.get(1))
                .withKeyValue("Attr0.0.0.1", randomValues.get(2))
                .withKeyValue("Attr3.1.0.0", randomValues.get(4))
                .withKeyValue("Attr3.1.0.1", randomValues.get(0));

        expectedOrderedFieldValues = givenExpectedOrderedFieldValues()
                .withKeyValue("Attr0", randomValues.get(0))
                .withKeyValue("Attr1", randomValues.get(1))
                .withKeyValue("Attr0", randomValues.get(2))
                .withKeyValue("Attr3", randomValues.get(4))
                .withKeyValue("Attr3", randomValues.get(0)).build();
        // @formatter:on

        givenValueTransformerForFields(UniqueGranularity.ALL, "Attr0", "Attr1", "Attr3");

        assertOrderedFieldValues();
    }

    private void assertUniqueDocuments() {
        List<Document> actual = getUniqueDocumentsWithUpdateConfigCalls(inputDocuments);
        Collections.sort(expectedUniqueDocuments);
        Collections.sort(actual);
        assertEquals("Unique documents do not match expected", expectedUniqueDocuments, actual);
    }

    private List<Document> getUniqueDocuments(List<Document> documents) {
        Transformer<Document,Map.Entry<Key,Document>> docToEntry = document -> Maps.immutableEntry(document.getMetadata(), document);
        TransformIterator<Document,Map.Entry<Key,Document>> inputIterator = new TransformIterator<>(documents.iterator(), docToEntry);
        UniqueTransform uniqueTransform = getUniqueTransform();
        Iterator<Map.Entry<Key,Document>> resultIterator = Iterators.transform(inputIterator, uniqueTransform);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false).filter(Objects::nonNull)
                        .map(Map.Entry::getValue).collect(Collectors.toList());
    }

    private List<Document> getUniqueDocumentsWithUpdateConfigCalls(List<Document> documents) {
        Transformer<Document,Map.Entry<Key,Document>> docToEntry = document -> Maps.immutableEntry(document.getMetadata(), document);
        TransformIterator<Document,Map.Entry<Key,Document>> inputIterator = new TransformIterator<>(documents.iterator(), docToEntry);
        UniqueTransform uniqueTransform = getUniqueTransform();
        Iterator<Map.Entry<Key,Document>> resultIterator = Iterators.transform(inputIterator, uniqueTransform);
        ArrayList<Document> docs = new ArrayList<>();
        while (resultIterator.hasNext()) {
            Map.Entry<Key,Document> next = resultIterator.next();
            if (next != null) {
                docs.add(next.getValue());
                updateUniqueTransform(uniqueTransform);
            }
        }
        return docs;
    }

    private void assertOrderedFieldValues() {
        try {
            UniqueTransform uniqueTransform = getUniqueTransform();
            for (Document d : inputDocuments) {
                assertEquals("Ordered field sets do not match expected", new String(expectedOrderedFieldValues, StandardCharsets.UTF_8),
                                new String(uniqueTransform.getBytes(d), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void givenValueTransformerForFields(UniqueGranularity transformer, String... fields) {
        Arrays.stream(fields).forEach((field) -> uniqueFields.put(field, transformer));
    }

    private void givenValueTransformersForField(String field, UniqueGranularity... transformers) {
        Arrays.stream(transformers).forEach((transformer) -> uniqueFields.put(field, transformer));
    }

    private UniqueTransform getUniqueTransform() {
        return new UniqueTransform(uniqueFields, Long.MAX_VALUE);
    }

    private void updateUniqueTransform(UniqueTransform uniqueTransform) {
        uniqueTransform.updateConfig(uniqueFields, null);
    }

    private InputDocumentBuilder givenInputDocument() {
        return new InputDocumentBuilder();
    }

    private InputDocumentBuilder givenInputDocument(String docKey) {
        return new InputDocumentBuilder(docKey);
    }

    private ExpectedOrderedFieldValuesBuilder givenExpectedOrderedFieldValues() {
        return new ExpectedOrderedFieldValuesBuilder();
    }

    private class InputDocumentBuilder {

        private final Document document;

        InputDocumentBuilder() {
            this.document = new Document();
            inputDocuments.add(document);
        }

        @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
        InputDocumentBuilder(String docKey) {

            Text MARKER_TEXT = new Text(docKey);
            ByteSequence MARKER_SEQUENCE = new ArrayByteSequence(MARKER_TEXT.getBytes(), 0, MARKER_TEXT.getLength());
            byte EMPTY_BYTES[] = new byte[0];
            Key key = new Key(EMPTY_BYTES, EMPTY_BYTES, MARKER_SEQUENCE.subSequence(0, MARKER_SEQUENCE.length()).toArray());
            this.document = new Document(key, true);
            inputDocuments.add(document);
            this.document.getMetadata().set(key);
        }

        @SuppressWarnings({"UnusedReturnValue", "SameParameterValue"})
        InputDocumentBuilder withRandomKeyValues(int minKeys, int maxKeys, int maxMultiValueKeys) {
            // Create random key-values.
            int totalKeys = random.nextInt((maxKeys + 1)) + minKeys;
            for (int i = 0; i < totalKeys; i++) {
                withKeyValue(getRandomKey(i), getRandomValue());
            }
            // Create multiple values for some of the keys.
            int multiValueKeys = Math.max(totalKeys, maxMultiValueKeys);
            for (int i = 0; i < multiValueKeys; i++) {
                withKeyValue(getRandomKey(i), getRandomValue());
            }
            return this;
        }

        private String getRandomKey(int index) {
            StringBuilder sb = new StringBuilder();
            if (random.nextBoolean()) {
                sb.append(JexlASTHelper.IDENTIFIER_PREFIX);
            }
            return sb.append("Attr").append(index).toString();
        }

        private String getRandomValue() {
            return randomValues.get(random.nextInt(randomValues.size()));
        }

        InputDocumentBuilder withKeyValue(String key, String value) {
            document.put(key, new DiacriticContent(value, document.getMetadata(), true), true, false);
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        InputDocumentBuilder isExpectedToBeUnique() {
            expectedUniqueDocuments.add(document);
            return this;
        }
    }

    private class ExpectedOrderedFieldValuesBuilder {

        private Multimap<String,String> fieldValues = TreeMultimap.create();

        ExpectedOrderedFieldValuesBuilder() {}

        ExpectedOrderedFieldValuesBuilder withKeyValue(String key, String value) {
            fieldValues.put(key, value);
            return this;
        }

        public byte[] build() {
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes);
                int count = 0;
                for (String field : fieldValues.keySet()) {
                    String separator = "f-" + field + '/' + (count++) + ":";
                    for (String value : fieldValues.get(field)) {
                        output.writeUTF(separator);
                        output.writeUTF(value);
                        separator = ",";
                    }
                }
                output.flush();
                return bytes.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}

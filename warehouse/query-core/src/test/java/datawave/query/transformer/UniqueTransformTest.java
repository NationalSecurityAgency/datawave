package datawave.query.transformer;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.hash.UID;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.DiacriticContent;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class UniqueTransformTest {
    private static final Logger log = Logger.getLogger(UniqueTransformTest.class);
    private List<String> values = new ArrayList();
    private List<String> visibilities = new ArrayList();
    
    @Before
    public void setup() {
        Random random = new Random(1000);
        for (int i = 0; i < 5; i++) {
            values.add(createValue(random, 10, 20));
        }
        for (int i = 0; i < 10; i++) {
            visibilities.add(createVisibility(random));
        }
    }
    
    private String createValue(Random random, int minSize, int maxSize) {
        int size = random.nextInt(maxSize - minSize + 1) + minSize;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            char c = (char) (random.nextInt('z' - '0' + 1) + '0');
            if (Character.isLetterOrDigit(c)) {
                builder.append(c);
            }
        }
        return builder.toString();
    }
    
    private String createVisibility(Random random) {
        int count = random.nextInt(5) + 2;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (builder.length() > 0) {
                builder.append('|');
            }
            builder.append(createValue(random, 3, 10));
        }
        return builder.toString();
    }
    
    private String createAttributeName(Random random, int index, boolean withGroups) {
        StringBuilder sb = new StringBuilder();
        if (random.nextBoolean()) {
            sb.append(JexlASTHelper.IDENTIFIER_PREFIX);
        }
        sb.append("Attr").append(index);
        if (withGroups && random.nextBoolean()) {
            sb.append('.').append(random.nextInt(2)).append('.').append(random.nextInt(2));
        }
        return sb.toString();
    }
    
    /**
     * Create a document with a set of fields with random values out of a static set
     * 
     * @return document
     */
    private Document createDocument(Random random, boolean withGroups) {
        Document d = new Document(createDocKey(random), true);
        // at least 10 attributes
        int attrs = random.nextInt(91) + 10;
        for (int i = 0; i < attrs; i++) {
            d.put(createAttributeName(random, i, withGroups), new DiacriticContent(values.get(random.nextInt(values.size())), d.getMetadata(), true),
                            withGroups, false);
        }
        // create multiple values for some of the attributes
        for (int i = 0; i < 50; i++) {
            d.put(createAttributeName(random, i, withGroups), new DiacriticContent(values.get(random.nextInt(values.size())), d.getMetadata(), true),
                            withGroups, false);
        }
        return d;
    }
    
    private byte[] createData(Random random, int min, int max) {
        int size = random.nextInt(max - min + 1) + min;
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }
    
    private Key createDocKey(Random random) {
        UID uid = UID.builder().newId(createData(random, 1000, 2000));
        return new Key("20180101_" + random.nextInt(10), "datatype\u0000" + uid, "",
                        new ColumnVisibility(visibilities.get(random.nextInt(visibilities.size()))), -1);
    }
    
    private int countUniqueness(List<Document> input, Set<String> fields) {
        Set<String> uniqueValues = new HashSet<>();
        for (Document d : input) {
            Multimap<String,String> values = HashMultimap.create();
            for (String docField : d.getDictionary().keySet()) {
                for (String field : fields) {
                    if (docField.toUpperCase().equals(field.toUpperCase()) || docField.toUpperCase().startsWith(field.toUpperCase() + '.')) {
                        Attribute a = d.get(docField);
                        if (a instanceof Attributes) {
                            for (Attribute c : ((Attributes) a).getAttributes()) {
                                values.put(field, String.valueOf(c.getData()));
                            }
                        } else {
                            values.put(field, String.valueOf(a.getData()));
                        }
                    }
                }
            }
            StringBuilder builder = new StringBuilder();
            List<String> docFields = new ArrayList<>(values.keySet());
            Collections.sort(docFields);
            for (String field : docFields) {
                List<String> docValues = new ArrayList<>(values.get(field));
                Collections.sort(docValues);
                if (builder.length() > 0) {
                    builder.append("/ ");
                }
                builder.append(field).append(':').append(Joiner.on(',').join(docValues));
            }
            uniqueValues.add(builder.toString());
        }
        return uniqueValues.size();
    }
    
    @Test
    public void testUniqueness() {
        Random random = new Random(2000);
        List<Document> input = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            input.add(createDocument(random, false));
        }
        
        // choose three fields such that the number of unique document is less than half the number of documents but greater than 10
        Set<String> fields = new HashSet<>();
        int expected = input.size();
        while (expected > input.size() / 2 || expected < 10) {
            fields.clear();
            while (fields.size() < 3) {
                fields.add("Attr" + random.nextInt(100));
            }
            expected = countUniqueness(input, fields);
        }
        
        Transformer docToEntry = input1 -> {
            Document d = (Document) input1;
            return Maps.immutableEntry(d.getMetadata(), d);
        };
        TransformIterator inputIterator = new TransformIterator(input.iterator(), docToEntry);
        fields = fields.stream().map(field -> (random.nextBoolean() ? '$' + field : field)).collect(Collectors.toSet());
        UniqueTransform transform = new UniqueTransform(fields);
        Iterator iter = Iterators.transform(inputIterator, transform);
        
        List<Object> eventList = Lists.newArrayList();
        while (iter.hasNext()) {
            Object next = iter.next();
            if (next != null) {
                eventList.add(next);
            }
        }
        
        Assert.assertEquals(expected, eventList.size());
        Assert.assertNull(transform.apply(null));
    }
    
    @Test
    public void testUniquenessForCaseInsensitivity() {
        List<Document> input = new ArrayList<>();
        List<Document> expected = new ArrayList<>();
        
        Document d = new Document();
        d.put("ATTR0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("ATTR0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("ATTR0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        input.add(d);
        
        d = new Document();
        d.put("Attr1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("Attr1", new DiacriticContent(values.get(3), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("Attr1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        input.add(d);
        
        d = new Document();
        d.put("attr2", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("attr2", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        input.add(d);
        expected.add(d);
        
        d = new Document();
        d.put("attr2", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        input.add(d);
        
        Set<String> fields = new HashSet<>(Arrays.asList("attr0", "Attr1", "ATTR2"));
        int expectedCount = countUniqueness(input, fields);
        
        Transformer docToEntry = input1 -> {
            Document doc = (Document) input1;
            return Maps.immutableEntry(doc.getMetadata(), doc);
        };
        TransformIterator inputIterator = new TransformIterator(input.iterator(), docToEntry);
        // fields = fields.stream().map(field -> (random.nextBoolean() ? '$' + field : field)).collect(Collectors.toSet());
        UniqueTransform transform = new UniqueTransform(fields);
        Iterator iter = Iterators.transform(inputIterator, transform);
        
        List<Document> eventList = Lists.newArrayList();
        while (iter.hasNext()) {
            // Object next = iter.next();
            Map.Entry<Key,Document> next = (Map.Entry<Key,Document>) iter.next();
            if (next != null) {
                eventList.add(next.getValue());
            }
        }
        
        Assert.assertEquals(expectedCount, eventList.size());
        Assert.assertEquals(expected, eventList);
    }
    
    /**
     * Test that groups get placed into separate field sets
     */
    @Test
    public void testUniquenessWithTwoGroups() {
        // create document two fields as follows:
        // field1.group1
        // field2.group1
        // field1.group2
        // field2.group2
        Document d = new Document();
        d.put("Attr0.0.0.0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        d.put("Attr0.0.0.1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.1", new DiacriticContent(values.get(3), d.getMetadata(), true), true, false);
        
        List<UniqueTransform.FieldSet> expected = new ArrayList<>();
        UniqueTransform.FieldSet set1 = new UniqueTransform.FieldSet();
        set1.put("Attr0", values.get(0));
        set1.put("Attr1", values.get(1));
        UniqueTransform.FieldSet set2 = new UniqueTransform.FieldSet();
        set2.put("Attr0", values.get(2));
        set2.put("Attr1", values.get(3));
        expected.add(set1);
        expected.add(set2);
        Collections.sort(expected);
        
        UniqueTransform transform = new UniqueTransform(Sets.newHashSet("Attr0", "Attr1"));
        List<UniqueTransform.FieldSet> fieldSets = transform.getOrderedFieldSets(d);
        Assert.assertEquals(expected, fieldSets);
    }
    
    /**
     * Test that groups get placed into separate field sets combined with ungrouped attributes
     */
    @Test
    public void testUniquenessWithTwoGroupsAndUngrouped() {
        // create document two fields as follows:
        // field1.group1
        // field1.group2
        // field2.group1
        // field2.group2
        // field3
        Document d = new Document();
        d.put("Attr0.0.0.0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        d.put("Attr0.0.0.1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.1", new DiacriticContent(values.get(3), d.getMetadata(), true), true, false);
        d.put("Attr3", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        
        List<UniqueTransform.FieldSet> expected = new ArrayList<>();
        UniqueTransform.FieldSet set1 = new UniqueTransform.FieldSet();
        set1.put("Attr0", values.get(0));
        set1.put("Attr1", values.get(1));
        set1.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set2 = new UniqueTransform.FieldSet();
        set2.put("Attr0", values.get(2));
        set2.put("Attr1", values.get(3));
        set2.put("Attr3", values.get(4));
        expected.add(set1);
        expected.add(set2);
        Collections.sort(expected);
        
        UniqueTransform transform = new UniqueTransform(Sets.newHashSet("Attr0", "Attr1", "Attr3"));
        List<UniqueTransform.FieldSet> fieldSets = transform.getOrderedFieldSets(d);
        Assert.assertEquals(expected, fieldSets);
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
        Document d = new Document();
        d.put("Attr0.0.0.0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        d.put("Attr0.0.0.1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.1", new DiacriticContent(values.get(3), d.getMetadata(), true), true, false);
        d.put("Attr3.1.0.0", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        
        List<UniqueTransform.FieldSet> expected = new ArrayList<>();
        UniqueTransform.FieldSet set1 = new UniqueTransform.FieldSet();
        set1.put("Attr0", values.get(0));
        set1.put("Attr1", values.get(1));
        set1.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set2 = new UniqueTransform.FieldSet();
        set2.put("Attr0", values.get(2));
        set2.put("Attr1", values.get(3));
        set2.put("Attr3", values.get(4));
        expected.add(set1);
        expected.add(set2);
        Collections.sort(expected);
        
        UniqueTransform transform = new UniqueTransform(Sets.newHashSet("Attr0", "Attr1", "Attr3"));
        List<UniqueTransform.FieldSet> fieldSets = transform.getOrderedFieldSets(d);
        Assert.assertEquals(expected, fieldSets);
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
        Document d = new Document();
        d.put("Attr0.0.0.0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        d.put("Attr0.0.0.1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.1", new DiacriticContent(values.get(3), d.getMetadata(), true), true, false);
        d.put("Attr3.1.0.0", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        d.put("Attr3.1.0.1", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        
        List<UniqueTransform.FieldSet> expected = new ArrayList<>();
        UniqueTransform.FieldSet set1 = new UniqueTransform.FieldSet();
        set1.put("Attr0", values.get(0));
        set1.put("Attr1", values.get(1));
        set1.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set2 = new UniqueTransform.FieldSet();
        set2.put("Attr0", values.get(2));
        set2.put("Attr1", values.get(3));
        set2.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set3 = new UniqueTransform.FieldSet();
        set3.put("Attr0", values.get(0));
        set3.put("Attr1", values.get(1));
        set3.put("Attr3", values.get(0));
        UniqueTransform.FieldSet set4 = new UniqueTransform.FieldSet();
        set4.put("Attr0", values.get(2));
        set4.put("Attr1", values.get(3));
        set4.put("Attr3", values.get(0));
        expected.add(set1);
        expected.add(set2);
        expected.add(set3);
        expected.add(set4);
        Collections.sort(expected);
        
        UniqueTransform transform = new UniqueTransform(Sets.newHashSet("Attr0", "Attr1", "Attr3"));
        List<UniqueTransform.FieldSet> fieldSets = transform.getOrderedFieldSets(d);
        Assert.assertEquals(expected, fieldSets);
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
        Document d = new Document();
        d.put("Attr0.0.0.0", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        d.put("Attr1.0.1.0", new DiacriticContent(values.get(1), d.getMetadata(), true), true, false);
        d.put("Attr0.0.0.1", new DiacriticContent(values.get(2), d.getMetadata(), true), true, false);
        d.put("Attr3.1.0.0", new DiacriticContent(values.get(4), d.getMetadata(), true), true, false);
        d.put("Attr3.1.0.1", new DiacriticContent(values.get(0), d.getMetadata(), true), true, false);
        
        List<UniqueTransform.FieldSet> expected = new ArrayList<>();
        UniqueTransform.FieldSet set1 = new UniqueTransform.FieldSet();
        set1.put("Attr0", values.get(0));
        set1.put("Attr1", values.get(1));
        set1.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set2 = new UniqueTransform.FieldSet();
        set2.put("Attr0", values.get(2));
        set2.put("Attr3", values.get(4));
        UniqueTransform.FieldSet set3 = new UniqueTransform.FieldSet();
        set3.put("Attr0", values.get(0));
        set3.put("Attr1", values.get(1));
        set3.put("Attr3", values.get(0));
        UniqueTransform.FieldSet set4 = new UniqueTransform.FieldSet();
        set4.put("Attr0", values.get(2));
        set4.put("Attr3", values.get(0));
        expected.add(set1);
        expected.add(set2);
        expected.add(set3);
        expected.add(set4);
        Collections.sort(expected);
        
        UniqueTransform transform = new UniqueTransform(Sets.newHashSet("Attr0", "Attr1", "Attr3"));
        List<UniqueTransform.FieldSet> fieldSets = transform.getOrderedFieldSets(d);
        Assert.assertEquals(expected, fieldSets);
    }
}

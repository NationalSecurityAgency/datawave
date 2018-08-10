package datawave.query.transformer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datawave.data.hash.UID;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.DiacriticContent;
import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class UniqueTransformTest {
    private static final Logger log = Logger.getLogger(UniqueTransformTest.class);
    private List<Document> input = new ArrayList<>();
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
        for (int i = 0; i < 100; i++) {
            input.add(createDocument(random));
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
    
    /**
     * Create a document with a set of fields with random values out of a static set
     * 
     * @return document
     */
    private Document createDocument(Random random) {
        Document d = new Document(createDocKey(random), true);
        // at least 10 attributes
        int attrs = random.nextInt(91) + 10;
        for (int i = 0; i < attrs; i++) {
            d.put("Attr" + i, new DiacriticContent(values.get(random.nextInt(values.size())), d.getMetadata(), true));
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
    
    private int countUniqueness(Set<String> fields) {
        Set<String> uniqueValues = new HashSet<String>();
        for (Document d : input) {
            StringBuilder builder = new StringBuilder();
            for (String field : fields) {
                if (builder.length() > 0) {
                    builder.append('\u0000');
                }
                Attribute a = d.get(field);
                if (a != null) {
                    builder.append(a.getData());
                }
            }
            uniqueValues.add(builder.toString());
        }
        return uniqueValues.size();
    }
    
    @Test
    public void testUniqueness() {
        // choose two fields such that the number of unique document is less than the number of documents.
        Set<String> fields = new HashSet<>();
        int expected = input.size();
        Random random = new Random(2000);
        while (expected > input.size() / 2) {
            fields.clear();
            while (fields.size() < 2) {
                fields.add("Attr" + random.nextInt(100));
            }
            expected = countUniqueness(fields);
        }
        
        Transformer docToEntry = new Transformer() {
            @Override
            public Object transform(Object input) {
                Document d = (Document) input;
                return Maps.immutableEntry(d.getMetadata(), d);
            }
        };
        TransformIterator inputIterator = new TransformIterator(input.iterator(), docToEntry);
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
}

package datawave.query.ancestor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import datawave.query.Constants;
import datawave.query.function.AncestorEquality;
import datawave.query.function.Equality;
import datawave.query.util.IteratorToSortedKeyValueIterator;

public class AncestorChildExpansionIteratorTest {
    private final List<String> children = Arrays.asList("a", "a.1", "a.1.1", "a.1.2", "a.1.2.1", "a.10", "a.2", "a.3", "a.4", "a.4.1", "a.4.1.1", "a.4.1.2",
                    "a.4.2", "a.5", "a.6", "a.7", "a.8", "a.9");

    private List<Map.Entry<Key,Value>> baseValues;
    private AncestorChildExpansionIterator iterator;
    private IteratorToSortedKeyValueIterator baseIterator;
    private Equality equality;

    @Before
    public void setup() {
        baseValues = new ArrayList<>();
        equality = new AncestorEquality();
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);
    }

    // basic iterator contract verification

    @Test
    public void testUninitializedHasTop() {
        assertThrows(IllegalStateException.class, () -> {
            iterator.hasTop();
        });
    }

    @Test
    public void testUninitializedgetTopKey() {
        assertThrows(IllegalStateException.class, () -> {
            iterator.getTopKey();
        });
    }

    @Test
    public void testUninitializedgetTopValue() {
        assertThrows(IllegalStateException.class, () -> {
            iterator.getTopValue();
        });
    }

    @Test
    public void testUninitializedNext() {
        assertThrows(IllegalStateException.class, () -> {
            iterator.next();
        });
    }

    @Test
    public void testSeekEnablesHasTop() throws IOException {
        iterator.seek(new Range(), Collections.EMPTY_LIST, false);
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testNoTopGetTopKeyError() {
        assertThrows(NoSuchElementException.class, () -> {
            iterator.seek(new Range(), Collections.EMPTY_LIST, false);
            assertFalse(".hasTop() returned true on EMPTY_LIST", iterator.hasTop());
            iterator.getTopKey();
        });
    }

    @Test
    public void testNoTopGetTopValueError() {
        assertThrows(NoSuchElementException.class, () -> {
            iterator.seek(new Range(), Collections.EMPTY_LIST, false);
            assertFalse(".hasTop() returned true on EMPTY_LIST", iterator.hasTop());
            iterator.getTopValue();
        });
    }

    @Test
    public void testNoTopNextError() {
        assertThrows(NoSuchElementException.class, () -> {
            iterator.seek(new Range(), Collections.EMPTY_LIST, false);
            assertFalse(".hasTop() returned true on EMPTY_LIST", iterator.hasTop());
            iterator.next();
        });
    }

    // end basic iterator contract verification

    private static final String FI_ROW = "shard";
    private static final String FI_COLUMN_FAMILY = "fi" + Constants.NULL_BYTE_STRING + "field";
    private static final String FI_COLUMN_QUALIFIER_PREFIX = "value" + Constants.NULL_BYTE_STRING + "dataType" + Constants.NULL_BYTE_STRING;
    private static final String FI_VIS = "ABC";
    private static final long FI_TIMESTAMP = 1234567890;

    private Key generateFiKey(String uid) {
        return new Key(FI_ROW, FI_COLUMN_FAMILY, FI_COLUMN_QUALIFIER_PREFIX + uid, FI_VIS, FI_TIMESTAMP);
    }

    private void assertKey(Key key, String uid) {
        assertNotNull(key);
        //  @formatter:off
        assertAll(
                () -> {assertEquals(key.getRow().toString(), FI_ROW);},
                () -> {assertEquals(key.getColumnFamily().toString(), FI_COLUMN_FAMILY);},
                () -> {assertEquals(key.getColumnQualifier().toString(), FI_COLUMN_QUALIFIER_PREFIX + uid);},
                () -> {assertEquals(new String(key.getColumnVisibilityParsed().getExpression()), FI_VIS);},
                () -> {assertEquals(key.getTimestamp(), FI_TIMESTAMP);}
        );
        //  @formatter:on
    }

    @Test
    public void testSingleChildMatch() throws IOException {
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.3"), new Value()));
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);

        iterator.seek(new Range(), Collections.EMPTY_LIST, false);

        assertTrue(iterator.hasTop());
        Key topKey = iterator.getTopKey();
        assertKey(topKey, "a.3");
        Value topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testFamilyMatch() throws IOException {
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.1"), new Value()));
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);

        iterator.seek(new Range(), Collections.EMPTY_LIST, false);

        assertTrue(iterator.hasTop());
        Key topKey = iterator.getTopKey();
        assertKey(topKey, "a.1");
        Value topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testFamilyMatchWithOverlaps() throws IOException {
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.1"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.1.1"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.1.2.1"), new Value()));
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);

        iterator.seek(new Range(), Collections.EMPTY_LIST, false);

        assertTrue(iterator.hasTop());
        Key topKey = iterator.getTopKey();
        assertKey(topKey, "a.1");
        Value topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testChildIndexAdvanceOnIteratorNext() throws IOException {
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.3"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.4.1"), new Value()));
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);

        iterator.seek(new Range(), Collections.EMPTY_LIST, false);

        assertTrue(iterator.hasTop());
        Key topKey = iterator.getTopKey();
        assertKey(topKey, "a.3");
        Value topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.4.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.4.1.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.4.1.2");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testMultipleGappedRanges() throws IOException {
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.1"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.3"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.4.1.1"), new Value()));
        baseValues.add(new AbstractMap.SimpleImmutableEntry<>(generateFiKey("a.9"), new Value()));
        baseIterator = new IteratorToSortedKeyValueIterator(baseValues.iterator());
        iterator = new AncestorChildExpansionIterator(baseIterator, children, equality);

        iterator.seek(new Range(), Collections.EMPTY_LIST, false);

        assertTrue(iterator.hasTop());
        Key topKey = iterator.getTopKey();
        assertKey(topKey, "a.1");
        Value topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.1.2.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.3");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.4.1.1");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertTrue(iterator.hasTop());
        topKey = iterator.getTopKey();
        assertKey(topKey, "a.9");
        topValue = iterator.getTopValue();
        assertNotNull(topValue);

        iterator.next();
        assertFalse(iterator.hasTop());
    }
}

package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

public class InMemoryIvaratorTest {
    private SortedListKeyValueIterator sourceItr;

    @BeforeEach
    public void setup() {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value())));
    }

    @Test
    public void DatawaveFieldIndexRegexIteratorJexl_inMemoryTest() throws IOException {
        DatawaveFieldIndexRegexIteratorJexl itr = DatawaveFieldIndexRegexIteratorJexl.builder().withFieldName("FIELD_A").withFieldValue(".*")
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.345"), itr.getTopKey());
        assertEquals(new Value(), itr.getTopValue());
    }

    @Test
    public void DatawaveFieldIndexRegexIteratorJexl_unsortedInMemoryTest() throws IOException {
        DatawaveFieldIndexRegexIteratorJexl itr = DatawaveFieldIndexRegexIteratorJexl.builder().withFieldName("FIELD_A").withFieldValue(".*")
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withSortedUUIDs(false).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.345", "FIELD_A\u0000a"), itr.getTopKey());
        assertEquals(new Value(), itr.getTopValue());
    }

    @Test
    public void DatawaveFieldIndexRegexIteratorJexl_inMemoryTimeoutTest() throws IOException {
        DatawaveFieldIndexRegexIteratorJexl itr = DatawaveFieldIndexRegexIteratorJexl.builder().withFieldName("FIELD_A").withFieldValue(".*")
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true)
                        // zero scan timeout guarantees an ivarator timeout
                        .withScanTimeout(0).build();

        itr.init(sourceItr, Map.of(), null);
        IvaratorException e = assertThrows(IvaratorException.class, () -> itr.seek(new Range(new Key("a"), true, null, true), null, true));
        assertEquals("Ivarator query timed out", e.getMessage());
    }

    @Test
    public void inMemoryMaxResultsTest() throws IOException {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "aa\u0000dataType\u0000123.234.345"), new Value())));

        DatawaveFieldIndexRegexIteratorJexl itr = DatawaveFieldIndexRegexIteratorJexl.builder().withFieldName("FIELD_A").withFieldValue(".*")
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true)
                        // zero scan timeout guarantees an ivarator timeout
                        .withMaxResults(1).build();

        itr.init(sourceItr, Map.of(), null);
        IvaratorException e = assertThrows(IvaratorException.class, () -> itr.seek(new Range(new Key("a"), true, null, true), null, true));
        assertEquals("Failed Ivarator fillSortedSets: java.util.concurrent.ExecutionException: java.lang.RuntimeException: datawave.query.exceptions.DatawaveIvaratorMaxResultsException: Exceeded the maximum set size",
                        e.getMessage());
    }

    // this test serves no purpose, without significant setup we can't force a yield
    @Test
    public void DatawaveFieldIndexRegexIteratorJexl_inMemoryYieldImpossibleTest() throws IOException {
        // yielding isn't possible when executing in memory because it uses a single thread
        DatawaveFieldIndexRegexIteratorJexl itr = DatawaveFieldIndexRegexIteratorJexl.builder().withFieldName("FIELD_A").withFieldValue(".*")
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
    }

    @Test
    public void DatawaveFieldIndexRangeIteratorJexl_range_inMemoryTest() throws IOException {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        // wrong field
                        Map.entry(new Key("a", "fi\u0000FIELD_B", "aa\u0000dataType\u0000123.234.345"), new Value()),
                        // outside the end range
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "zz\u0000dataType\u0000124.234.345"), new Value())));

        DatawaveFieldIndexRangeIteratorJexl itr = DatawaveFieldIndexRangeIteratorJexl.builder().withFieldName("FIELD_A").withLowerBound("a")
                        .lowerInclusive(true).withUpperBound("z").upperInclusive(true)
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.345"), itr.getTopKey());
        itr.next();
        assertFalse(itr.hasTop());
    }

    @Test
    public void DatawaveFieldIndexRangeIteratorJexl_subRange_inMemoryTest() throws IOException {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "aa\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "f\u0000dataType\u0000124.234.345"), new Value())));

        SortedSet<Range> subRanges = new TreeSet<>();
        subRanges.add(new Range("e", true, "g", true));
        subRanges.add(new Range("a", true, "c", true));
        DatawaveFieldIndexRangeIteratorJexl itr = DatawaveFieldIndexRangeIteratorJexl.builder().withFieldName("FIELD_A").withLowerBound("a")
                        .lowerInclusive(true).withUpperBound("z").upperInclusive(true).withSubRanges(subRanges)
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        // value a
        assertTrue(itr.hasTop());
        itr.next();
        // value aa is skipped because it's the same uid
        // value f has a different uid
        assertTrue(itr.hasTop());
        itr.next();
        // nothing left
        assertFalse(itr.hasTop());
    }

    @Test
    public void DatawaveFieldIndexRangeIteratorJexl_subRangeUnsorted_inMemoryTest() throws IOException {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "aa\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "f\u0000dataType\u0000124.234.345"), new Value())));

        SortedSet<Range> subRanges = new TreeSet<>();
        subRanges.add(new Range("e", true, "g", true));
        subRanges.add(new Range("a", true, "c", true));
        DatawaveFieldIndexRangeIteratorJexl itr = DatawaveFieldIndexRangeIteratorJexl.builder().withFieldName("FIELD_A").withLowerBound("a")
                        .lowerInclusive(true).withUpperBound("z").upperInclusive(true).withSubRanges(subRanges)
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).withSortedUUIDs(false).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        // value a
        assertTrue(itr.hasTop());
        itr.next();
        // value aa is skipped because it's the same uid
        assertTrue(itr.hasTop());
        itr.next();
        // value f has a different uid
        assertTrue(itr.hasTop());
        itr.next();
        // nothing left
        assertFalse(itr.hasTop());
    }

    @Test
    public void DatawaveFieldIndexFilterIteratorJexl_inMemoryTest() throws Exception {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        // filtered
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "aa\u0000dataType\u0000123.234.345"), new Value()),
                        // outside the end range
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "zz\u0000dataType\u0000124.234.345"), new Value())));

        DatawaveFieldIndexFilterIteratorJexl itr = DatawaveFieldIndexFilterIteratorJexl.builder().withFieldName("FIELD_A").withLowerBound("a")
                        .lowerInclusive(true).withUpperBound("z").upperInclusive(true).withFilter(k -> {
                            // only return the first key
                            return k.getColumnQualifier().toString().startsWith("a\u0000");
                        }).withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.345"), itr.getTopKey());
        itr.next();
        assertFalse(itr.hasTop());
    }

    @Test
    public void DatawaveFieldIndexListIteratorJexl_values_inMemoryTest() throws Exception {
        sourceItr = new SortedListKeyValueIterator(List.of(Map.entry(new Key("a", "fi\u0000FIELD_A", "a\u0000dataType\u0000123.234.345"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "aa\u0000dataType\u0000123.234.346"), new Value()),
                        // filtered
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "ab\u0000dataType\u0000123.234.341"), new Value()),
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "z\u0000dataType\u0000124.234.347"), new Value()),
                        // filtered
                        Map.entry(new Key("a", "fi\u0000FIELD_A", "zz\u0000dataType\u0000124.234.342"), new Value())));

        DatawaveFieldIndexListIteratorJexl itr = DatawaveFieldIndexListIteratorJexl.builder().withFieldName("FIELD_A").withValues(List.of("a", "aa", "z"))
                        .withIvaratorSourcePool(new GenericObjectPool<>(new BasePoolableObjectFactory<>() {
                            @Override
                            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                                return sourceItr.deepCopy(null);
                            }
                        })).withWaitWindowObserver(new WaitWindowObserver()).withLimitLookup(true).build();

        itr.init(sourceItr, Map.of(), null);
        itr.seek(new Range(new Key("a"), true, null, true), null, true);

        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.345"), itr.getTopKey());
        itr.next();
        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000123.234.346"), itr.getTopKey());
        itr.next();
        assertTrue(itr.hasTop());
        assertEquals(new Key("a", "dataType\u0000124.234.347"), itr.getTopKey());
        itr.next();
        assertFalse(itr.hasTop());
    }
}

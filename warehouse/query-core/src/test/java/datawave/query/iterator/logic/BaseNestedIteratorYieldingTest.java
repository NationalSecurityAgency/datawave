package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.waitwindow.WaitWindowObserver;
import datawave.query.jexl.JexlNodeFactory;

/**
 * Collection of core methods used by {@link NestedIterator} tests that exercise yielding
 */
public class BaseNestedIteratorYieldingTest {

    private static final Logger log = LoggerFactory.getLogger(BaseNestedIteratorYieldingTest.class);

    protected static final Value EMPTY_VALUE = new Value();

    protected final Random random = new Random();

    // collection of uid sets used by extending classes
    protected SortedSet<String> uidsA = new TreeSet<>();
    protected SortedSet<String> uidsB = new TreeSet<>();
    protected SortedSet<String> uidsC = new TreeSet<>();
    protected SortedSet<String> uidsD = new TreeSet<>();

    protected void rebuildUids() {
        uidsA = randomUids();
        uidsB = randomUids();
        uidsC = randomUids();
        uidsD = randomUids();
    }

    /**
     * Generate a set of random uids. The cardinality is randomly selected between low, medium and high.
     *
     * @return a sorted set of uids
     */
    protected SortedSet<String> randomUids() {
        return randomUids(25, random.nextInt(20));
    }

    /**
     * Generates a set of random uids. The number of uids must be lower than the bound
     *
     * @param bound
     *            the upper bound
     * @param numUids
     *            the number of uids to generate
     * @return a set of random uids
     */
    protected SortedSet<String> randomUids(int bound, int numUids) {
        Preconditions.checkArgument(numUids >= 0, "cannot request negative uids");
        Preconditions.checkArgument(numUids <= bound, "cannot request more uids than upper bound");

        SortedSet<String> uids = new TreeSet<>();
        while (uids.size() < numUids) {
            int i = 1 + random.nextInt(bound);
            uids.add(String.valueOf(i));
        }
        return uids;
    }

    /**
     * Used by tests to define a tree of iterators for a test
     */
    protected interface IteratorSupplier {
        NestedIterator<Key> get(WaitWindowObserver observer);
    }

    /**
     * Used by tests to define how expected uids are calculated
     */
    protected interface UidSupplier {
        SortedSet<String> get();
    }

    /**
     * Creates a leaf iterator for the provided field and uids
     *
     * @param field
     *            the field
     * @param uids
     *            the uids
     * @return a NestedIterator
     */
    protected NestedIterator<Key> createIterator(String field, SortedSet<String> uids) {
        Text textField = new Text(field);
        Text textValue = new Text("value");
        SortedKeyValueIterator<Key,Value> source = createSource(field, uids);
        //  @formatter:off
        IndexIterator indexIterator = IndexIterator.builder(textField, textValue, source)
                //  always build documents for tests so we can assert results
                .shouldBuildDocument(true)
                .build();
        //  @formatter:on

        JexlNode node = JexlNodeFactory.buildEQNode(field, "value");
        IndexIteratorBridge bridge = new IndexIteratorBridge(indexIterator, node, field);
        // building documents for index only fields, must set the non-event field flag
        bridge.setNonEventField(true);
        return bridge;
    }

    /**
     * Creates the source which is a {@link SortedMapIterator}
     *
     * @param field
     *            the field
     * @param uids
     *            the uids
     * @return the source iterator
     */
    protected SortedKeyValueIterator<Key,Value> createSource(String field, SortedSet<String> uids) {
        SortedMap<Key,Value> sourceData = createSourceData(field, uids);
        return new SortedMapIterator(sourceData);
    }

    /**
     * Create source data from a set of uids
     *
     * @param field
     *            the field
     * @param uids
     *            the uids
     * @return the source data
     */
    protected SortedMap<Key,Value> createSourceData(String field, SortedSet<String> uids) {
        SortedMap<Key,Value> source = new TreeMap<>();
        for (String uid : uids) {
            source.put(createFiKey(field, uid), EMPTY_VALUE);
        }
        return source;
    }

    /**
     * Creates the field index key for the provided field and uid
     *
     * @param field
     *            the field
     * @param uid
     *            the uid
     * @return a field index key
     */
    protected Key createFiKey(String field, String uid) {
        String row = "20250606_0";
        String cf = "fi\0" + field;
        String cq = "value\0datatype\0" + uid;
        return new Key(row, cf, cq);
    }

    /**
     * Drive the NestedIterator provided by the {@link IteratorSupplier} and assert the results match the {@link UidSupplier}
     *
     * @param observer
     *            the WaitWindowObserver
     * @param iteratorSupplier
     *            the IteratorSupplier
     * @param uidSupplier
     *            the UidSupplier
     * @throws Exception
     *             if something goes wrong
     */
    protected void drive(WaitWindowObserver observer, IteratorSupplier iteratorSupplier, UidSupplier uidSupplier) throws Exception {
        Range range = new Range();
        SortedSet<String> results = new TreeSet<>();
        boolean completed = false;
        while (!completed) {
            try {
                // start/restart observer after each yield to reset counts
                observer.start("qid", Long.MAX_VALUE);
                observer.setSeekRange(range);
                log.trace("seek to range: {}", range);
                NestedIterator<Key> iter = iteratorSupplier.get(observer);
                iter.seek(range, Collections.emptySet(), false);
                iter.initialize();
                while (iter.hasNext()) {
                    Key tk = iter.next();
                    String cf = tk.getColumnFamily().toString();
                    String uid = cf.substring(cf.indexOf('\u0000') + 1);
                    boolean newResult = results.add(uid);
                    assertTrue(newResult, "duplicate uid: " + uid);
                }
            } catch (WaitWindowOverrunException e) {
                Pair<Key,String> pair = e.getYieldKey();
                Key start = pair.getLeft();
                // null check required because a yield during the initial seek will not have a yield key set
                if (start != null) {
                    start = WaitWindowObserver.removeMarkers(start);
                    log.trace("yielded at {}", start.toStringNoTime());
                    range = new Range(start, true, range.getEndKey(), range.isEndKeyInclusive());
                }
                continue;
            }
            completed = true;
        }

        SortedSet<String> expected = uidSupplier.get();
        if (!expected.equals(results)) {
            Set<String> missing = Sets.difference(expected, results);
            Set<String> unexpected = Sets.difference(results, expected);
            if (!missing.isEmpty()) {
                log.info("missing uids {}", missing);
            }
            if (!unexpected.isEmpty()) {
                log.info("unexpected uids {}", unexpected);
            }
        }

        assertEquals(expected, results);
    }
}

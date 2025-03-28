package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

public class RegexFilterIteratorTest {

    private static final Value value = new Value();

    private String field;
    private String pattern;

    private SortedKeyValueIterator<Key,Value> source;
    private RegexFilterIterator iter;

    @BeforeEach
    public void beforeEach() {
        // force iterator rebuild with each test's field and pattern
        iter = null;
    }

    // full matrix, every key should match
    @Test
    public void testFieldA() {
        withFieldPattern("FIELD_A", "val.*");
        for (String datatype : List.of("a", "b", "c")) {
            for (String uid : List.of("1", "2", "3")) {
                String target = "datatype-" + datatype + "\0uid-" + uid;
                move(true, target);
            }
        }
    }

    // narrow 1x1 matrix
    @Test
    public void testFieldB() {
        withFieldPattern("FIELD_B", "val.*");

        // the only hit
        move(true, "datatype-b\0uid-2");

        // all misses
        for (String datatype : List.of("a", "c")) {
            for (String uid : List.of("1", "2", "3")) {
                String target = "datatype-" + datatype + "\0uid-" + uid;
                move(false, target);
            }
        }
    }

    // narrow 1x1 matrix but with more values
    @Test
    public void testFieldC() {
        withFieldPattern("FIELD_C", "val.*");

        // the only hit
        move(true, "datatype-b\0uid-2");

        // all misses
        for (String datatype : List.of("a", "c")) {
            for (String uid : List.of("1", "2", "3")) {
                String target = "datatype-" + datatype + "\0uid-" + uid;
                move(false, target);
            }
        }
    }

    // 3x1 matrix of datatypes to uids
    @Test
    public void testFieldD() {
        withFieldPattern("FIELD_D", "val.*");

        for (String datatype : List.of("a", "b", "c")) {
            // all datatypes hit on uid 2
            // all datatypes miss on uid 1 and 3
            move(false, "datatype-" + datatype + "\0uid-1");
            move(true, "datatype-" + datatype + "\0uid-2");
            move(false, "datatype-" + datatype + "\0uid-3");
        }
    }

    // 1x3 matrix of datatypes to uids
    @Test
    public void testFieldE() {
        withFieldPattern("FIELD_E", "val.*");

        // datatype-a and datatype-c have no hits
        for (String uid : List.of("1", "2", "3")) {
            move(false, "datatype-a\0uid-" + uid);
            move(false, "datatype-c\0uid-" + uid);
        }

        // datatype-b hits on every uid
        move(true, "datatype-b\0uid-1");
        move(true, "datatype-b\0uid-2");
        move(true, "datatype-b\0uid-3");
    }

    @Test
    public void testInvalidPattern() {
        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E")) {
            withFieldPattern(field, "miss.*");

            for (String datatype : List.of("a", "b", "c")) {
                for (String uid : List.of("1", "2", "3")) {
                    String target = "datatype-" + datatype + "\0uid-" + uid;
                    move(false, target);
                }
            }

            iter = null; // force iterator rebuild with fresh field/pattern
        }
    }

    @Test
    public void testInvalidFields() {
        for (String field : List.of("FIELD_X", "FIELD_Y", "FIELD_Z")) {
            withFieldPattern(field, "val.*");

            for (String datatype : List.of("a", "b", "c")) {
                for (String uid : List.of("1", "2", "3")) {
                    String target = "datatype-" + datatype + "\0uid-" + uid;
                    move(false, target);
                }
            }

            iter = null; // force iterator rebuild with fresh field/pattern
        }
    }

    @Test
    public void testInvalidDatatypes() {
        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E")) {
            withFieldPattern(field, "value.*");

            for (String datatype : List.of("x", "y", "z")) {
                for (String uid : List.of("1", "2", "3")) {
                    String target = "datatype-" + datatype + "\0uid-" + uid;
                    move(false, target);
                }
            }

            iter = null; // force iterator rebuild with fresh field/pattern
        }
    }

    @Test
    public void testInvalidUids() {
        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E")) {
            withFieldPattern(field, "val.*");

            for (String datatype : List.of("a", "b", "c")) {
                for (String uid : List.of("0", "5", "7")) {
                    String target = "datatype-" + datatype + "\0uid-" + uid;
                    move(false, target);
                }
            }

            iter = null; // force iterator rebuild with fresh field/pattern
        }
    }

    /**
     * Simulate a call to {@link RegexFilterIterator#move(Key)} and assert the expected result
     *
     * @param expected
     *            the expected result
     * @param datatypeUid
     *            the datatype and uid
     */
    private void move(boolean expected, String datatypeUid) {
        RegexFilterIterator filter = getIterator();

        Key target = createMoveTarget(datatypeUid);
        Key result = filter.move(target);
        assertEquals(expected, result.equals(target));
    }

    private Key createMoveTarget(String datatypeUid) {
        return new Key("row", datatypeUid);
    }

    private RegexFilterIterator getIterator() {
        if (iter == null) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(pattern);

            iter = new RegexFilterIterator().withField(field).withPattern(pattern).withAggregation(false).withSource(getSource());
        }
        return iter;
    }

    private void withFieldPattern(String field, String pattern) {
        this.field = field;
        this.pattern = pattern;
    }

    private SortedKeyValueIterator<Key,Value> getSource() {
        if (source == null) {
            source = createData();
        }
        return source;
    }

    private SortedMapIterator createData() {

        SortedMap<Key,Value> data = new TreeMap<>();

        // FIELD_A is a full 3x3 matrix of values, datatypes, and uids
        List<String> values = List.of("a", "b", "c");
        List<String> datatypes = List.of("a", "b", "c");
        List<String> uids = List.of("1", "2", "3");
        load(data, "FIELD_A", values, datatypes, uids);

        // FIELD_B is a 1x1 matrix
        values = List.of("b");
        datatypes = List.of("b");
        uids = List.of("2");
        load(data, "FIELD_B", values, datatypes, uids);

        // FIELD_C is all values, single datatype, single uid
        values = List.of("a", "b", "c");
        datatypes = List.of("b");
        uids = List.of("2");
        load(data, "FIELD_C", values, datatypes, uids);

        // FIELD_D is single value, all datatypes, single uid
        values = List.of("b");
        datatypes = List.of("a", "b", "c");
        uids = List.of("2");
        load(data, "FIELD_D", values, datatypes, uids);

        // FIELD_E is single value, single datatype, all uids
        values = List.of("b");
        datatypes = List.of("b");
        uids = List.of("1", "2", "3");
        load(data, "FIELD_E", values, datatypes, uids);

        return new SortedMapIterator(data);
    }

    private void load(SortedMap<Key,Value> data, String field, List<String> values, List<String> datatypes, List<String> uids) {
        String cf = "fi\0" + field;
        for (String part : values) {
            for (String dt : datatypes) {
                for (String uid : uids) {
                    String cq = "value-" + part + "\0datatype-" + dt + "\0uid-" + uid;
                    data.put(new Key("row", cf, cq, "PUBLIC", 12L), value);
                }
            }
        }
    }
}

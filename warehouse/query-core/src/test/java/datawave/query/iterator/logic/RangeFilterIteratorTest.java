package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;

public class RangeFilterIteratorTest {

    private static final Value value = new Value();

    private SortedKeyValueIterator<Key,Value> source;
    private RangeFilterIterator iter;

    private String range;

    @BeforeEach
    public void setup() {
        iter = null;
    }

    @Test
    public void testFullRange() {
        withRange("((_Bounded_ = true) && (FOO >= '1' && FOO <= '9'))");
        scan(true, "all", "even", "odd", "prime");
    }

    @Test
    public void testSmallRange_greaterThanEqual_lessThanEqual() {
        withRange("((_Bounded_ = true) && (FOO >= '1' && FOO <= '2'))");
        scan(true, "all", "even", "odd", "prime");
    }

    @Test
    public void testSmallRange_greaterThanEqual_lessThan() {
        withRange("((_Bounded_ = true) && (FOO >= '1' && FOO < '2'))");
        scan(true, "all", "odd");
        scan(false, "even", "prime");
    }

    @Test
    public void testSmallRange_greaterThan_lessThanEqual() {
        withRange("((_Bounded_ = true) && (FOO > '1' && FOO <= '2'))");
        scan(false, "odd");
        scan(true, "all", "even", "prime");
    }

    @Test
    public void testSmallRange_greaterThan_lessThan() {
        withRange("((_Bounded_ = true) && (FOO > '1' && FOO < '2'))");
        scan(false, "all", "even", "odd", "prime");
    }

    @Test
    public void testRangeTooPreciseNoData() {
        withRange("((_Bounded_ = true) && (FOO > '551' && FOO < '552'))");
        scan(false, "all", "even", "odd", "prime");
    }

    @Test
    public void testInvalidField() {
        withRange("((_Bounded_ = true) && (ZEE > '3' && ZEE < '5'))");
        scan(false, "all", "even", "odd", "prime");
    }

    @Test
    public void testInvalidRange() {
        withRange("((_Bounded_ = true) && (FOO > 'a' && FOO < 'b'))");
        scan(false, "all", "even", "odd", "prime");
    }

    @Test
    public void testInvalidDatatypes() {
        withRange("((_Bounded_ = true) && (FOO >= '1' && FOO <= '9'))");
        scan(false, "none", "imaginary", "quadratic");
    }

    private void scan(boolean expected, String... datatypes) {
        for (String datatype : datatypes) {
            for (String uid : List.of("uid-1", "uid-2", "uid-3", "uid-4", "uid-5")) {
                String target = datatype + '\u0000' + uid;
                move(expected, target);
            }
        }
    }

    private void withRange(String range) {
        this.range = range;
    }

    /**
     * Simulate a call to {@link RangeFilterIterator#move(Key)} and assert the expected result
     *
     * @param expected
     *            the expected result
     * @param datatypeUid
     *            the datatype and uid
     */
    private void move(boolean expected, String datatypeUid) {
        RangeFilterIterator filter = getIterator();

        Key target = createMoveTarget(datatypeUid);
        Key result = filter.move(target);
        assertEquals(expected, result.equals(target));
    }

    private Key createMoveTarget(String datatypeUid) {
        return new Key("row", datatypeUid);
    }

    private RangeFilterIterator getIterator() {
        if (iter == null) {
            JexlNode node = parse(range).jjtGetChild(0);
            LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);

            iter = new RangeFilterIterator().withField(range.getFieldName()).withLowerBound(range.getLower().toString())
                            .withUpperBound(range.getUpper().toString()).withLowerInclusive(range.isLowerInclusive())
                            .withUpperInclusive(range.isUpperInclusive()).withAggregation(false).withSource(getSource());
        }
        return iter;
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse: " + query);
            throw new RuntimeException(e);
        }
    }

    private SortedKeyValueIterator<Key,Value> getSource() {
        if (source == null) {
            source = createData();
        }
        return source;
    }

    /**
     * Data for range iterator relies more on values than datatypes and uids
     *
     * @return range data
     */
    private SortedMapIterator createData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        load(data, "1", List.of("all", "odd"));
        load(data, "2", List.of("all", "even", "prime"));
        load(data, "3", List.of("all", "odd", "prime"));
        load(data, "4", List.of("all", "even"));
        load(data, "5", List.of("all", "odd", "prime"));
        load(data, "6", List.of("all", "even"));
        load(data, "7", List.of("all", "odd", "prime"));
        load(data, "8", List.of("all", "even"));
        load(data, "9", List.of("all", "odd"));
        return new SortedMapIterator(data);
    }

    private void load(SortedMap<Key,Value> data, String val, List<String> datatypes) {
        String cf = "fi\0FOO";
        List<String> uids = List.of("1", "2", "3", "4", "5");
        for (String datatype : datatypes) {
            for (String uid : uids) {
                String cq = val + "\0" + datatype + "\0uid-" + uid;
                data.put(new Key("row", cf, cq, "PUBLIC", 12L), value);
            }
        }
    }
}

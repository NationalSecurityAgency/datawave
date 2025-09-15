package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.QueryPlan;

class DayIndexIteratorTest {

    private final BitSet even = BitSetFactory.create(0, 2, 4, 6, 8);
    private final BitSet odd = BitSetFactory.create(1, 3, 5, 7, 9);

    private String query;
    private String day;
    private final Map<String,BitSet> termsToShards = new HashMap<>();

    private final Map<String,String> expected = new HashMap<>();

    @BeforeEach
    public void before() {
        query = null;
        day = null;
        termsToShards.clear();
        expected.clear();
    }

    @Test
    public void testEven() {
        setQuery("F == 'even'");
        setDay("20240101");
        addShard("F == 'even'", even);

        addExpectedShards("20240101", "F == 'even'", 0, 2, 4, 6, 8);

        driveIterator();
    }

    @Test
    public void testOdd() {
        setQuery("F == 'odd'");
        setDay("20240101");
        addShard("F == 'odd'", odd);

        addExpectedShards("20240101", "F == 'odd'", 1, 3, 5, 7, 9);

        driveIterator();
    }

    @Test
    public void testEvenAndOdd() {
        setQuery("F == 'even' && F == 'odd'");
        setDay("20240101");
        addShard("F == 'even'", even);
        addShard("F == 'odd'", odd);

        // no intersection
        driveIterator();
    }

    @Test
    public void testEvenOrOdd() {
        setQuery("F == 'even' || F == 'odd'");
        setDay("20240101");
        addShard("F == 'even'", even);
        addShard("F == 'odd'", odd);

        addExpectedShards("20240101", "F == 'even'", 0, 2, 4, 6, 8);
        addExpectedShards("20240101", "F == 'odd'", 1, 3, 5, 7, 9);

        // no intersection
        driveIterator();
    }

    private void driveIterator() {
        ASTJexlScript script = getQuery();
        DayIndexIterator iterator = new DayIndexIterator(script);

        assertNotNull(day);
        iterator.setDay(day);

        assertNotNull(termsToShards);
        iterator.setShards(termsToShards);

        int expectedCount = expected.size();
        int count = 0;
        while (iterator.hasNext()) {
            QueryPlan plan = iterator.next();
            String shard = plan.getRanges().iterator().next().getStartKey().getRow().toString();
            String queryString = plan.getQueryString();
            boolean contained = expected.remove(shard, queryString);
            assertTrue(contained, "expected map did not contain entry for {" + shard + "," + queryString + "}");
            count++;
        }

        assertEquals(expectedCount, count);
    }

    private ASTJexlScript getQuery() {
        assertNotNull(query);
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Could not parse query: " + query);
            throw new RuntimeException(e);
        }
    }

    private void setQuery(String query) {
        this.query = query;
    }

    private void setDay(String day) {
        this.day = day;
    }

    private void addShard(String term, BitSet shards) {
        termsToShards.put(term, shards);
    }

    private void addExpected(String shard, String query) {
        expected.put(shard, query);
    }

    private void addExpectedShards(String day, String query, int... shards) {
        for (int i : shards) {
            addExpected(day + "_" + i, query);
        }
    }
}

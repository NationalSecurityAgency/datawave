package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.TimeFilter;

/**
 * Simple IT to verify structure of And/Or Iterators
 */
class IteratorBuildingVisitorIT {

    private static final Value EMPTY_VALUE = new Value();

    private final Set<String> fields = Sets.newHashSet("A", "B", "Z");

    @Test
    void testSimpleUnion() {
        String query = "(A == '1' || B == '2')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // includes
    }

    @Test
    void testSimpleUnionWithNegation() {
        String query = "(A == '1' || !(B == '2'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // include + contextExclude
    }

    @Test
    void testSimpleUnionOfNegatedTerms() {
        // root cannot be a negation
        String query = "(!(A == '1') || !(B == '2'))";
        NestedIterator<Key> root = visit(query);
        assertNull(root);
        // root is null because all top level terms are negated, but no exception was thrown
    }

    @Test
    void testNegatedUnion() {
        // root cannot be a negation. The pushdown visitor would have handled this
        String query = "!(A == '1' || B == '2')";
        assertThrows(IllegalStateException.class, () -> visit(query));
    }

    @Test
    void testSimpleIntersection() {
        String query = "(A == '1' && B == '2')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // includes
    }

    @Test
    void testIntersectionWithNegation() {
        String query = "(A == '1' && !(B == '2'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // include + exclude
    }

    @Test
    void testIntersectionOfNegatedTerms() {
        String query = "(!(A == '1') && !(B == '2'))";
        NestedIterator<Key> root = visit(query);
        assertNull(root);
        // root is null because all top level terms are negated, but no exception was thrown
    }

    @Test
    void testNegatedIntersection() {
        // root cannot be negation
        String query = "!(A == '1' && B == '2')";
        assertThrows(IllegalStateException.class, () -> visit(query));
    }

    @Test
    void testIntersectionWithNestedUnion() {
        String query = "A == '1' && (B == '2' || B == '3')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // two includes
    }

    @Test
    void testIntersectionWithNestedUnionWithNegatedTerm() {
        String query = "A == '1' && (B == '2' || !(B == '3'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // includes + context includes
    }

    @Test
    void testIntersectionWithNestedUnionOfNegatedTerms() {
        String query = "A == '1' && (!(B == '2') || !(B == '3'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // or = context excludes
        // and = include + context exclude
    }

    // technically shouldn't get here due to pushdown negations
    @Test
    void testIntersectionWithNegatedNestedUnion() {
        String query = "A == '1' && !(B == '2' || B == '3')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // or = includes
        // and = include + exclude
    }

    @Test
    void testUnionWithNestedIntersection() {
        String query = "A == '1' || (B == '2' && B == '3')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // includes
    }

    @Test
    void testUnionWithNestedIntersectionWithNegatedTerm() {
        String query = "A == '1' || (B == '2' && !(B == '3'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // and = include + exclude
        // or = includes
    }

    @Test
    void testUnionWithNestedIntersectionOfNegatedTerms() {
        String query = "A == '1' || (!(B == '2') && !(B == '3'))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // and = not built because it would be an effective top level intersection
        // or = single include for A term
    }

    // foil to the above case
    @Test
    void testDeeplyNestedIntersectionOfNegatedTerms() {
        String query = "Z == '7' && (A == '1' || (!(B == '2') && !(B == '3')))";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // top level intersection of Z term allows the deeply nested intersection of negated B terms
        // to be added to a union
        // root intersection = include + context include
        // union = include + context exclude
        // nested intersection = excludes
    }

    @Test
    void testUnionWithNegatedNestedIntersection() {
        // PushdownNegationVisitor would change this query
        String query = "A == '1' || !(B == '2' && B == '3')";
        NestedIterator<Key> root = visit(query);
        assertNotNull(root);
        // and = includes
        // or = include + context exclude
    }

    private NestedIterator<Key> visit(String query) {
        ASTJexlScript script = parseQuery(query);

        IteratorBuildingVisitor visitor = new IteratorBuildingVisitor();

        IteratorEnvironment env = new TestIteratorEnvironment();
        visitor.setSource(new SourceFactory(getSource()), env);
        // type metadata?

        visitor.setTimeFilter(TimeFilter.alwaysTrue());
        visitor.setLimitLookup(true);
        visitor.setRange(new Range());

        visitor.setFieldsToAggregate(fields);
        visitor.setIndexOnlyFields(fields);
        visitor.setTermFrequencyFields(fields);

        script.jjtAccept(visitor, null);
        return visitor.root();
    }

    private ASTJexlScript parseQuery(String query) {
        try {
            return JexlASTHelper.parseJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query + " " + e.getMessage());
        }
        return null;
    }

    private Iterator<Map.Entry<Key,Value>> getSource() {
        SortedSet<String> uids = new TreeSet<>(List.of("a", "b", "c", "d", "e"));
        return createSourceData("FIELD", uids).entrySet().iterator();
    }

    protected static SortedMap<Key,Value> createSourceData(String field, SortedSet<String> uids) {
        SortedMap<Key,Value> source = new TreeMap<>();
        for (String uid : uids) {
            source.put(createFiKey(field, uid), EMPTY_VALUE);
        }
        return source;
    }

    protected static Key createFiKey(String field, String uid) {
        String row = "20220314_17";
        String cf = "fi\0" + field;
        String cq = "value\0datatype\0" + uid;
        return new Key(row, cf, cq);
    }

    private static class SourceFactory implements datawave.query.iterator.SourceFactory<Key,Value> {
        private Iterator<Map.Entry<Key,Value>> iterator;

        public SourceFactory(Iterator<Map.Entry<Key,Value>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public SortedKeyValueIterator<Key,Value> getSourceDeepCopy() {
            return new SortedListKeyValueIterator(iterator);
        }
    }

    private static class TestIteratorEnvironment implements IteratorEnvironment {
        public boolean isSamplingEnabled() {
            return false;
        }
    }
}

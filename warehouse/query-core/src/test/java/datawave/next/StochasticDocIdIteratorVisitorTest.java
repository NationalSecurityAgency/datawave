package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.next.ModeledQuery.Term;

/**
 * Drives random queries built from terms that are executable, non-executable or negated, and checks the visitor against {@link ModeledQuery} rather than a hand
 * written expectation.
 * <p>
 * Each iteration asserts two things: the visitor bounds the query exactly when the model says the field index can, and when it can, the candidates match
 * exactly. Where the model says it cannot, the visitor owes a refusal, not an empty set.
 */
public class StochasticDocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    private static final Logger log = LoggerFactory.getLogger(StochasticDocIdIteratorVisitorTest.class);

    private final Set<String> fields = Set.of("FIELD_A", "FIELD_B");
    private final Set<String> datatypes = Set.of("datatype-a");

    /** every uid {@link #getRandomUids()} can draw */
    private static final Set<Integer> UNIVERSE = Set.of(1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009);

    private final List<String> executableTerms = new ArrayList<>();
    private final List<String> nonExecutableTerms = new ArrayList<>();
    private final List<String> allTerms = new ArrayList<>();

    private final Range range = new Range(row);
    private final Random rand = new Random();

    // the maximum number of iterations each test should run
    private final int max = 1_000;

    @BeforeEach
    public void beforeEach() {
        executableTerms.clear();
        executableTerms.add("FIELD_A == 'value-1'");
        executableTerms.add("FIELD_A == 'value-2'");
        executableTerms.add("FIELD_B == 'value-3'");
        executableTerms.add("FIELD_B == 'value-4'");

        nonExecutableTerms.clear();
        nonExecutableTerms.add("FIELD_X == 'x'");
        nonExecutableTerms.add("FIELD_Y == 'y'");
        nonExecutableTerms.add("FIELD_Z == 'z'");
        nonExecutableTerms.add("filter:isNull(FIELD_X, 'x')");
        nonExecutableTerms.add("filter:isNotNull(FIELD_X, 'x')");

        allTerms.clear();
        allTerms.addAll(executableTerms);
        allTerms.addAll(nonExecutableTerms);
    }

    @Test
    public void testRandomSingleTerms() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(1);
            drive(ModeledQuery.term(terms[0]));
        }
    }

    /**
     * A bare negation, which the field index can never bound: there is no way to enumerate the documents that do not match a term.
     */
    @Test
    public void testRandomNegatedSingleTerms() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(1);
            drive(ModeledQuery.negated(terms[0]));
        }
    }

    @Test
    public void testIntersections() {
        for (int i = 0; i < max; i++) {
            driveIntersection(2);
            driveIntersection(3);
            driveIntersection(4);
        }
    }

    @Test
    public void testUnions() {
        for (int i = 0; i < max; i++) {
            driveUnion(2);
            driveUnion(3);
            driveUnion(4);
        }
    }

    // A || (B && C)
    @Test
    public void testNestedIntersection() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(3);
            drive(ModeledQuery.or(ModeledQuery.term(terms[0]), ModeledQuery.and(ModeledQuery.term(terms[1]), ModeledQuery.term(terms[2]))));
        }
    }

    // A && (B || C)
    @Test
    public void testNestedUnion() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(3);
            drive(ModeledQuery.and(ModeledQuery.term(terms[0]), ModeledQuery.or(ModeledQuery.term(terms[1]), ModeledQuery.term(terms[2]))));
        }
    }

    // A && B && ... && !Z
    @Test
    public void testIntersectionWithNegations() {
        for (int i = 0; i < max; i++) {
            driveIntersectionWithNegation(2);
            driveIntersectionWithNegation(3);
            driveIntersectionWithNegation(4);
        }
    }

    /**
     * A || !B, which the field index can never bound: the union offers no candidate set for the negation to be subtracted from.
     */
    @Test
    public void testUnionWithNegations() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(2);
            drive(ModeledQuery.or(ModeledQuery.term(terms[0]), ModeledQuery.negated(terms[1])));
        }
    }

    // A || (B && !C)
    @Test
    public void testNestedIntersectionWithNegations() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(3);
            drive(ModeledQuery.or(ModeledQuery.term(terms[0]), ModeledQuery.and(ModeledQuery.term(terms[1]), ModeledQuery.negated(terms[2]))));
        }
    }

    // A && (B || !C)
    @Test
    public void testNestedUnionWithNegations() {
        for (int i = 0; i < max; i++) {
            Term[] terms = pickTerms(3);
            drive(ModeledQuery.and(ModeledQuery.term(terms[0]), ModeledQuery.or(ModeledQuery.term(terms[1]), ModeledQuery.negated(terms[2]))));
        }
    }

    private void driveIntersection(int termCount) {
        drive(ModeledQuery.and(positiveSlots(pickTerms(termCount))));
    }

    private void driveUnion(int termCount) {
        drive(ModeledQuery.or(positiveSlots(pickTerms(termCount))));
    }

    private void driveIntersectionWithNegation(int termCount) {
        Term[] terms = pickTerms(termCount);

        ModeledQuery[] slots = new ModeledQuery[termCount];
        for (int i = 0; i < termCount - 1; i++) {
            slots[i] = ModeledQuery.term(terms[i]);
        }
        slots[termCount - 1] = ModeledQuery.negated(terms[termCount - 1]);

        drive(ModeledQuery.and(slots));
    }

    private ModeledQuery[] positiveSlots(Term[] terms) {
        ModeledQuery[] slots = new ModeledQuery[terms.length];
        for (int i = 0; i < terms.length; i++) {
            slots[i] = ModeledQuery.term(terms[i]);
        }
        return slots;
    }

    /**
     * Runs one query and checks it against the model, both in whether the index can bound it at all and in the candidates it produces
     *
     * @param model
     *            the query form
     */
    private void drive(ModeledQuery model) {
        ASTJexlScript script = parse(model.query());
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, fields);
        ScanResult result = visitor.getScanResult(script);

        boolean expectBound = model.bound() != null;
        boolean actualBound = result != null && result.isBounded();
        if (expectBound != actualBound) {
            logState(model);
        }
        assertEquals(expectBound, actualBound, "wrong answer on whether the index can bound: " + model.query());

        if (!expectBound) {
            // the visitor owes a refusal rather than a set, which getDocIds turns into a fatal query exception
            return;
        }

        Set<Integer> expected = model.candidates(UNIVERSE);
        SortedSet<Integer> actual = resultsToUids(result.getResults());
        if (!expected.equals(actual)) {
            logState(model);
        }
        assertEquals(expected, actual, model.query());
    }

    /**
     * Selects distinct random terms and writes index data for the ones the field index can resolve
     *
     * @param count
     *            the number of terms
     * @return the modeled terms
     */
    private Term[] pickTerms(int count) {
        clearState();

        Set<String> selected = new HashSet<>();
        Term[] terms = new Term[count];
        for (int i = 0; i < count; i++) {
            String query = selectRandomTerm(selected);
            if (executableTerms.contains(query)) {
                Set<Integer> uids = getRandomUids();
                writeUidsForTerm(query, uids);
                terms[i] = Term.resolved(query, uids);
            } else {
                terms[i] = Term.unresolved(query);
            }
        }
        return terms;
    }

    private String selectRandomTerm(Set<String> selected) {
        while (true) {
            String term = allTerms.get(rand.nextInt(allTerms.size()));
            if (selected.add(term)) {
                return term;
            }
        }
    }

    private void writeUidsForTerm(String term, Set<Integer> uids) {
        String[] parts = term.split(" ");
        for (Integer uid : uids) {
            String field = parts[0];
            String value = parts[2].substring(1, parts[2].length() - 1);
            writeIndex(field, value, "datatype-a", uid);
        }
    }

    private Set<Integer> getRandomUids() {
        int count = rand.nextInt(10);
        Set<Integer> uids = new TreeSet<>();
        while (uids.size() < count) {
            uids.add(1000 + rand.nextInt(10));
        }
        return uids;
    }

    private SortedSet<Integer> resultsToUids(Set<Key> results) {
        SortedSet<Integer> uids = new TreeSet<>();
        for (Key result : results) {
            String cq = result.getColumnFamily().toString();
            int index = cq.lastIndexOf('-');
            String uid = cq.substring(index + 1);
            uids.add(Integer.parseInt(uid) - 1_000);
        }
        return uids;
    }

    private void logState(ModeledQuery model) {
        log.info("query: {}", model.query());
        log.info("shape: {}", model.shape());
        log.info("bound: {}", model.bound());
        log.info("candidates: {}", model.candidates(UNIVERSE));
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}

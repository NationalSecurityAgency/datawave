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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * This test exercises random terms are executable, not executable, or negated.
 */
public class StochasticDocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    private static final Logger log = LoggerFactory.getLogger(StochasticDocIdIteratorVisitorTest.class);

    private static final Set<String> fields = Set.of("FIELD_A", "FIELD_B");
    private static final Set<String> datatypes = Set.of("datatype-a");

    private static final List<String> executableTerms = new ArrayList<>();
    private static final List<String> nonExecutableTerms = new ArrayList<>();
    private static final List<String> allTerms = new ArrayList<>();

    static {
        executableTerms.add("FIELD_A == 'value-1'");
        executableTerms.add("FIELD_A == 'value-2'");
        executableTerms.add("FIELD_B == 'value-3'");
        executableTerms.add("FIELD_B == 'value-4'");
        nonExecutableTerms.add("FIELD_X == 'x'");
        nonExecutableTerms.add("FIELD_Y == 'y'");
        nonExecutableTerms.add("FIELD_Z == 'z'");
        nonExecutableTerms.add("filter:isNull(FIELD_X, 'x')");
        nonExecutableTerms.add("filter:isNotNull(FIELD_X, 'x')");
        nonExecutableTerms.add("FIELD_X !~ 'ba.*'");
        nonExecutableTerms.add("FIELD_X != 'x'");
        allTerms.addAll(executableTerms);
        allTerms.addAll(nonExecutableTerms);
    }

    private final Range range = new Range(row);
    private final Random rand = new Random();

    private final Set<String> selectedTerms = new HashSet<>();

    private String[] terms;
    private Set<Integer>[] uids;
    private Set<Integer> expected = null;

    // the maximum number of iterations each test should run
    private final int max = 1_000;

    @Test
    public void testRandomSingleTerms() {
        for (int i = 0; i < max; i++) {
            driveSingleTerm();
        }
    }

    @Test
    public void testIntersections() {
        for (int i = 0; i < max; i++) {
            driveSimpleIntersection(2);
            driveSimpleIntersection(3);
            driveSimpleIntersection(4);
        }
    }

    @Test
    public void testUnions() {
        for (int i = 0; i < max; i++) {
            driveSimpleUnion(2);
            driveSimpleUnion(3);
            driveSimpleUnion(4);
        }
    }

    @Test
    public void testNestedIntersection() {
        for (int i = 0; i < max; i++) {
            driveNestedIntersection();
        }
    }

    @Test
    public void testNestedUnion() {
        for (int i = 0; i < max; i++) {
            driveNestedUnion();
        }
    }

    @Test
    public void testIntersectionWithNegations() {
        for (int i = 0; i < max; i++) {
            driveIntersectionWithNegations(2);
            driveIntersectionWithNegations(3);
            driveIntersectionWithNegations(4);
        }
    }

    @Test
    public void testUnionWithNegations() {
        // this cannot happen without outside context, see testNestedUnionWithNegations
    }

    @Test
    public void testNestedIntersectionWithNegations() {
        for (int i = 0; i < max; i++) {
            driveNestedIntersectionWithNegation();
        }
    }

    @Disabled
    @Test
    public void testNestedUnionWithNegations() {
        for (int i = 0; i < max; i++) {
            driveNestedUnionWithNegation();
        }
    }

    private void driveSingleTerm() {
        clearState();

        String term = selectRandomTerm();
        withQuery(term);
        if (!isTermExecutable(term)) {
            return;
        }

        SortedSet<Integer> uids = getRandomUids();
        writeUidsForTerm(term, uids);
        expected = uids;
        driveTest();
    }

    // (A && B && ... && Z)
    private void driveSimpleIntersection(int termCount) {
        clearState();
        createTermsAndUids(termCount);
        buildExpectedForSimpleIntersection();
        withQuery(Joiner.on(" && ").join(terms));
        driveTest();
    }

    private void buildExpectedForSimpleIntersection() {
        for (int i = 0; i < terms.length; i++) {
            String term = terms[i];
            if (isTermExecutable(term)) {
                if (expected == null) {
                    expected = new HashSet<>(uids[i]);
                } else {
                    expected.retainAll(uids[i]);
                    if (expected.isEmpty()) {
                        break;
                    }
                }
            }
        }

        // possible no term was executable
        if (expected == null) {
            expected = new HashSet<>();
        }
    }

    // (A || B || ... || Z)
    private void driveSimpleUnion(int termCount) {
        clearState();
        createTermsAndUids(termCount);
        withQuery(Joiner.on(" || ").join(terms));
        buildExpectedForSimpleUnion();
        driveTest();
    }

    private void buildExpectedForSimpleUnion() {
        expected = new HashSet<>();
        for (int i = 0; i < terms.length; i++) {
            if (isTermExecutable(terms[i])) {
                expected.addAll(uids[i]);
            }
        }
    }

    // A || (B && C)
    private void driveNestedIntersection() {
        clearState();
        createTermsAndUids(3);
        buildExpectedForNestedIntersection();

        // A || (B && C)
        withQuery(terms[0] + " || (" + terms[1] + " && " + terms[2] + ")");
        driveTest();
    }

    private void buildExpectedForNestedIntersection() {
        expected = new HashSet<>();
        if (isTermExecutable(terms[1]) && isTermExecutable(terms[2])) {
            expected.addAll(uids[2]);
            expected.retainAll(uids[1]);
        } else if (isTermExecutable(terms[1])) {
            expected.addAll(uids[1]);
        } else if (isTermExecutable(terms[2])) {
            expected.addAll(uids[2]);
        }

        if (isTermExecutable(terms[0])) {
            expected.addAll(uids[0]);
        }
    }

    private void driveNestedUnion() {
        clearState();
        createTermsAndUids(3);
        buildExpectedForNestedUnion();

        // A && (B || C)
        withQuery(terms[0] + " && (" + terms[1] + " || " + terms[2] + ")");
        driveTest();
    }

    private void buildExpectedForNestedUnion() {
        expected = new HashSet<>();
        if (isTermExecutable(terms[2])) {
            expected.addAll(uids[2]);
        }
        if (isTermExecutable(terms[1])) {
            expected.addAll(uids[1]);
        }

        if (isTermExecutable(terms[0])) {
            if (!expected.isEmpty()) {
                expected.retainAll(uids[0]);
            } else if (!isTermExecutable(terms[1]) && !isTermExecutable(terms[2])) {
                expected.addAll(uids[0]);
            }
        }
    }

    // (A && B && ... !Z)
    private void driveIntersectionWithNegations(int termCount) {
        clearState();
        createTermsAndUids(termCount);
        buildExpectedForIntersectionWithNegations();
        negateLastTerm();
        withQuery(Joiner.on(" && ").join(terms));
        driveTest();
    }

    private void buildExpectedForIntersectionWithNegations() {
        // process terms using intersection logic, except the last term
        for (int i = 0; i < terms.length - 1; i++) {
            if (isTermExecutable(terms[i])) {
                if (expected == null) {
                    expected = new TreeSet<>(uids[i]);
                } else {
                    expected.retainAll(uids[i]);
                }
            }
        }

        // process negated intersection logic for last term, provided expected uids exist
        // and last term is executable
        if (expected != null && !expected.isEmpty() && isTermExecutable(terms[terms.length - 1])) {
            expected.removeAll(uids[uids.length - 1]);
        }

        if (expected == null) {
            expected = new TreeSet<>();
        }
    }

    // A || (B && !C)
    private void driveNestedIntersectionWithNegation() {
        clearState();
        createTermsAndUids(3);
        buildExpectedForNestedIntersectionWithNegation();
        negateLastTerm();
        withQuery(terms[0] + " || (" + terms[1] + " && " + terms[2] + ")");
        driveTest();
    }

    private void buildExpectedForNestedIntersectionWithNegation() {
        // A || (B && !C)
        expected = new HashSet<>();
        if (isTermExecutable(terms[1])) {
            expected.addAll(uids[1]);
            if (isTermExecutable(terms[2])) {
                expected.removeAll(uids[2]);
            }
        }
        // If B term is not executable we actually have a union like (A || !C)
        // in this case do nothing for the C term, it isn't executable. Visitor should ignore.

        if (isTermExecutable(terms[0])) {
            expected.addAll(uids[0]);
        }
    }

    // A && (B || !C)
    // query may take the above form, logically equivalent to:
    // (A && B) || (A && !C)
    private void driveNestedUnionWithNegation() {
        clearState();
        createTermsAndUids(3);
        buildExpectedForNestedUnionWithNegation();
        negateLastTerm();
        withQuery(terms[0] + " && (" + terms[1] + " || " + terms[2] + ")");
        driveTest();
    }

    private void buildExpectedForNestedUnionWithNegation() {
        Set<Integer> left = new HashSet<>(uids[0]);
        left.retainAll(uids[1]);

        Set<Integer> right = new HashSet<>(uids[0]);
        right.removeAll(uids[2]);

        expected = new HashSet<>();
        expected.addAll(left);
        expected.addAll(right);
    }

    private void driveTest() {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(expected);

        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        Set<Key> results = DocIdIteratorVisitor.getDocIds(script, range, source, datatypes, null, fields);
        SortedSet<Integer> resultUids = resultsToUids(results);

        assertEquals(expected, resultUids);
    }

    protected void clearState() {
        super.clearState();
        selectedTerms.clear();
        query = null;
        terms = null;
        uids = null;
        expected = null;
    }

    /**
     * The number of positive terms to create
     *
     * @param count
     *            the term count
     */
    @SuppressWarnings("unchecked")
    protected void createTermsAndUids(int count) {
        terms = new String[count];
        uids = new Set[count];

        for (int i = 0; i < count; i++) {
            terms[i] = selectRandomTerm();
            if (isTermExecutable(terms[i])) {
                uids[i] = getRandomUids();
            } else {
                uids[i] = new HashSet<>();
            }
        }

        writeData();
    }

    protected void negateLastTerm() {
        Preconditions.checkNotNull(terms);
        terms[terms.length - 1] = negateTerm(terms[terms.length - 1]);
    }

    protected void writeData() {
        for (int i = 0; i < terms.length; i++) {
            if (isTermExecutable(terms[i])) {
                writeUidsForTerm(terms[i], uids[i]);
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

    private String negateTerm(String term) {
        return "!(" + term + ")";
    }

    private String selectRandomTerm() {
        String term = null;
        boolean found = false;
        while (!found) {
            term = allTerms.get(rand.nextInt(allTerms.size()));
            if (!selectedTerms.contains(term)) {
                selectedTerms.add(term);
                found = true;
            }
        }
        return term;
    }

    private boolean isTermExecutable(String term) {
        return executableTerms.contains(term);
    }

    private SortedSet<Integer> getRandomUids() {
        int count = rand.nextInt(10);
        SortedSet<Integer> uids = new TreeSet<>();
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

    private void logState() {
        for (int i = 0; i < terms.length; i++) {
            log.info("{} :: {}", terms[i], uids[i]);
        }
        log.info("expected: {}", expected);
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Not implemented");
    }
}

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

import com.google.common.base.Joiner;

public class StochasticDocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    private static final Set<String> fields = Set.of("FIELD_A", "FIELD_B");
    private static final Set<String> datatypes = Set.of("datatype-a");

    private static final List<String> executableTerms = List.of("FIELD_A == 'value-1'", "FIELD_A == 'value-2'", "FIELD_B == 'value-3'", "FIELD_B == 'value-4'");
    private static final List<String> nonExecutableTerms = List.of("FIELD_X == 'x'", "FIELD_Y == 'y'", "FIELD_Z == 'z'");
    private static final List<String> allTerms = new ArrayList<>();

    static {
        allTerms.addAll(executableTerms);
        allTerms.addAll(nonExecutableTerms);
    }

    private final Range range = new Range(row);
    private final Random rand = new Random();

    private final List<String> termsForTest = new ArrayList<>();

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

    @Disabled
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

    @Disabled
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
        driveTest(uids);
    }

    // (A && B && ... && Z)
    private void driveSimpleIntersection(int termCount) {
        clearState();

        String[] termArray = new String[termCount];
        Set<Integer>[] uidArray = new Set[termCount];

        Set<String> terms = new HashSet<>();
        Set<Integer> expectedUids = null;

        for (int i = 0; i < termArray.length; i++) {
            termArray[i] = selectRandomTerm();
            uidArray[i] = getRandomUids();
            writeUidsForTerm(termArray[i], uidArray[i]);

            if (isTermExecutable(termArray[i])) {
                if (expectedUids == null) {
                    expectedUids = new HashSet<>(uidArray[i]);
                } else {
                    expectedUids.retainAll(uidArray[i]);
                }
            }
            terms.add(termArray[i]);
        }

        // possible no term was executable
        if (expectedUids == null) {
            expectedUids = new HashSet<>();
        }

        withQuery(Joiner.on(" && ").join(terms));

        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        Set<Key> results = DocIdIteratorVisitor.getDocIds(script, range, source, datatypes, null, fields);
        SortedSet<Integer> resultUids = resultsToUids(results);

        assertEquals(expectedUids, resultUids);
    }

    // (A || B || ... || Z)
    private void driveSimpleUnion(int termCount) {
        clearState();

        String[] termArray = new String[termCount];
        Set<Integer>[] uidArray = new Set[termCount];

        Set<String> terms = new HashSet<>();
        Set<Integer> expectedUids = new HashSet<>();

        for (int i = 0; i < termArray.length; i++) {
            termArray[i] = selectRandomTerm();
            uidArray[i] = getRandomUids();
            writeUidsForTerm(termArray[i], uidArray[i]);

            terms.add(termArray[i]);
            if (isTermExecutable(termArray[i])) {
                expectedUids.addAll(uidArray[i]);
            }
        }

        withQuery(Joiner.on(" || ").join(terms));

        driveTest(expectedUids);
    }

    // A || (B && C)
    private void driveNestedIntersection() {
        clearState();

        String[] terms = new String[3];
        Set<Integer>[] uidArray = new Set[3];

        for (int i = 0; i < terms.length; i++) {
            terms[i] = selectRandomTerm();
            uidArray[i] = getRandomUids();
            writeUidsForTerm(terms[i], uidArray[i]);
        }

        // A || (B && C)
        withQuery(terms[0] + " || (" + terms[1] + " && " + terms[2] + ")");

        Set<Integer> expectedUids = new HashSet<>();

        if (isTermExecutable(terms[1]) && isTermExecutable(terms[2])) {
            expectedUids.addAll(uidArray[2]);
            expectedUids.retainAll(uidArray[1]);
        } else if (isTermExecutable(terms[1])) {
            expectedUids.addAll(uidArray[1]);
        } else if (isTermExecutable(terms[2])) {
            expectedUids.addAll(uidArray[2]);
        }

        if (isTermExecutable(terms[0])) {
            expectedUids.addAll(uidArray[0]);
        }

        driveTest(expectedUids);
    }

    private void driveNestedUnion() {
        clearState();

        String[] terms = new String[3];
        Set<Integer>[] uidArray = new Set[3];

        for (int i = 0; i < terms.length; i++) {
            terms[i] = selectRandomTerm();
            uidArray[i] = getRandomUids();
            writeUidsForTerm(terms[i], uidArray[i]);
        }

        // A && (B || C)
        withQuery(terms[0] + " && (" + terms[1] + " || " + terms[2] + ")");

        Set<Integer> expectedUids = new HashSet<>();
        if (isTermExecutable(terms[2])) {
            expectedUids.addAll(uidArray[2]);
        }
        if (isTermExecutable(terms[1])) {
            expectedUids.addAll(uidArray[1]);
        }

        if (isTermExecutable(terms[0])) {
            if (!expectedUids.isEmpty()) {
                expectedUids.retainAll(uidArray[0]);
            } else if (!isTermExecutable(terms[1]) && !isTermExecutable(terms[2])) {
                expectedUids.addAll(uidArray[0]);
            }
        }

        driveTest(expectedUids);
    }

    // (A && B && ... !Z)
    private void driveIntersectionWithNegations(int termCount) {
        clearState();

        String[] termArray = new String[termCount];
        Set<Integer>[] uidArray = new Set[termCount];

        Set<String> terms = new HashSet<>();
        Set<Integer> expectedUids = new HashSet<>();

        for (int i = 0; i < termArray.length; i++) {
            termArray[i] = selectRandomTerm();
            if (i == termArray.length - 1) {
                // just negate the last one, for now
                termArray[i] = negateTerm(termArray[i]);
            }
            uidArray[i] = getRandomUids();
            writeUidsForTerm(termArray[i], uidArray[i]);

            if (i == 0) {
                expectedUids.addAll(uidArray[i]);
            } else if (i == termArray.length - 1) {
                expectedUids.removeAll(uidArray[i]);
            } else {
                expectedUids.retainAll(uidArray[i]);
            }
            terms.add(termArray[i]);
        }

        withQuery(Joiner.on(" && ").join(terms));
        driveTest(expectedUids);
    }

    // A || (B && !C)
    private void driveNestedIntersectionWithNegation() {
        clearState();

        String[] termArray = new String[3];
        Set<Integer>[] uidArray = new Set[3];

        Set<String> terms = new HashSet<>();
        for (int i = 0; i < termArray.length; i++) {
            termArray[i] = selectRandomTerm();
            if (i == termArray.length - 1) {
                // just negate the last one, for now
                termArray[i] = negateTerm(termArray[i]);
            }
            uidArray[i] = getRandomUids();
            writeUidsForTerm(termArray[i], uidArray[i]);
            terms.add(termArray[i]);
        }

        Set<Integer> expectedUids = new HashSet<>();
        expectedUids.addAll(uidArray[1]);
        expectedUids.removeAll(uidArray[2]);
        expectedUids.addAll(uidArray[0]);

        withQuery(termArray[0] + " || (" + termArray[1] + " && " + termArray[2] + ")");
        driveTest(expectedUids);
    }

    // A && (B || !C)
    // query may take the above form, logically equivalent to:
    // (A && B) || (A && !C)
    private void driveNestedUnionWithNegation() {
        clearState();

        String[] termArray = new String[3];
        Set<Integer>[] uidArray = new Set[3];

        Set<String> terms = new HashSet<>();
        for (int i = 0; i < termArray.length; i++) {
            termArray[i] = selectRandomTerm();
            if (i == termArray.length - 1) {
                // just negate the last one, for now
                termArray[i] = negateTerm(termArray[i]);
            }
            uidArray[i] = getRandomUids();
            writeUidsForTerm(termArray[i], uidArray[i]);
            terms.add(termArray[i]);
        }

        Set<Integer> left = new HashSet<>(uidArray[0]);
        left.retainAll(uidArray[1]);

        Set<Integer> right = new HashSet<>(uidArray[0]);
        right.removeAll(uidArray[2]);

        Set<Integer> expectedUids = new HashSet<>();
        expectedUids.addAll(left);
        expectedUids.addAll(right);

        withQuery(termArray[0] + " && (" + termArray[1] + " || " + termArray[2] + ")");
        driveTest(expectedUids);
    }

    private void driveTest(Set<Integer> expectedUids) {
        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        Set<Key> results = DocIdIteratorVisitor.getDocIds(script, range, source, datatypes, null, fields);
        SortedSet<Integer> resultUids = resultsToUids(results);

        assertEquals(expectedUids, resultUids);
    }

    protected void clearState() {
        super.clearState();
        termsForTest.clear();
        query = null;
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
            if (!termsForTest.contains(term)) {
                termsForTest.add(term);
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

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Not implemented");
    }
}

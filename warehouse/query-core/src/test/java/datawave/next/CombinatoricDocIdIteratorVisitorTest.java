package datawave.next;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.next.ModeledQuery.Term;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;

/**
 * Sweeps every combination of five query forms -- single term, union, intersection, nested union and nested intersection -- against a catalogue of terms
 * covering equality, regex, bounded range, markers, non-indexed fields and functions, in both polarities.
 * <p>
 * Expected results are modelled by {@link ModeledQuery} rather than hand written. Every combination is checked three ways: the visitor bounds the query exactly
 * when the model says it can, no uid the query could match is missing, and none it cannot match is present. Missing one is a false negative in the query
 * results, since a uid dropped here is never evaluated against its document.
 * <p>
 * Only combinations the planner could produce are swept. {@link #unreachable(String)} discards any the {@link ExecutableDeterminationVisitor} does not call
 * executable, and throws on any the {@link PushdownNegationVisitor} rewrites, since that is a term the catalogue should never have built. Every slot uses a
 * different field and value, see {@link #distinct(ModeledQuery...)}.
 * <p>
 * Results are grouped by shape, naming how the visitor handles each slot: R resolved, U unresolvable, M an unresolvable marker. {@code (R && (R || U))} is an
 * anchor intersected with a union of a resolvable and an opaque term. A shape that misbehaves while absent from {@link #DROPS_CANDIDATES} or
 * {@link #RETURNS_EXTRA_CANDIDATES} fails as a regression, and a listed shape that stops misbehaving fails as a gap to remove.
 */
public class CombinatoricDocIdIteratorVisitorTest extends FieldIndexDataTestUtil {

    private final Range range = new Range(row);

    // @formatter:off
    private static final Set<String> INDEXED_FIELDS = Set.of(
                    "FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E", "FIELD_F", "FIELD_G", "FIELD_H", "FIELD_I", "FIELD_J");
    // @formatter:on

    /** every uid written by the fixture */
    private static final Set<Integer> UNIVERSE = Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

    /**
     * Shapes where the visitor drops candidates the query can match. Each would be a false negative in the query results, since a uid that never becomes a
     * candidate is never evaluated against its document, so the list is expected to stay empty.
     */
    private static final Set<String> DROPS_CANDIDATES = Set.of();

    /**
     * Shapes where the visitor returns candidates the query cannot match. These are safe, since a candidate is evaluated against its document before it reaches
     * the user, but each one is a document fetched and evaluated for nothing.
     */
    private static final Set<String> RETURNS_EXTRA_CANDIDATES = Set.of();

    // @formatter:off
    private static final Term EQ_A = Term.resolved("FIELD_A == 'value-a'", 1, 2, 3, 4, 5, 6, 7, 8);
    private static final Term EQ_B = Term.resolved("FIELD_B == 'value-b'", 5, 6, 7, 8, 9, 10, 11, 12);
    private static final Term EQ_C = Term.resolved("FIELD_C == 'value-c'", 2, 4, 6, 8, 10, 12);
    private static final Term REGEX = Term.resolved("FIELD_D =~ 'match.*'", 1, 3, 5, 7, 9, 11);
    private static final Term RANGE = Term.resolved("((_Bounded_ = true) && (FIELD_E >= 'b' && FIELD_E <= 'd'))", 3, 4, 5, 6, 7, 8);
    private static final Term LIST = Term.resolved(
                    "((_List_ = true) && (((id = 'uuid') && (field = 'FIELD_F') && (params = '{\"values\":[\"one\",\"three\"]}'))))",
                    1, 2, 3, 7, 8, 9);
    private static final Term VALUE = Term.resolved("((_Value_ = true) && (FIELD_G =~ 'gv.*'))", 2, 3, 4, 9, 10);
    // the query model wraps an expanded field in a leniency marker, which only says how a missing field is evaluated and leaves the source scannable
    private static final Term LENIENT = Term.resolved("((_Lenient_ = true) && (FIELD_I == 'value-i'))", 1, 4, 5, 8, 9, 12);

    // there is deliberately no != or !~ term: RewriteNegationsVisitor turns those into !(==) and !(=~) while planning, so an ASTNENode or ASTNRNode cannot
    // reach the field index, and DocIdIteratorVisitor now fails the scan rather than accepting one
    private static final Term NON_INDEXED = Term.unresolved("NON_INDEXED_A == 'value-h'");
    private static final Term FUNCTION = Term.unresolved("filter:isNull(FIELD_J, 'value-k')");
    private static final Term NON_INDEXED_RANGE = Term.unresolved("((_Bounded_ = true) && (NON_INDEXED_B >= 'a' && NON_INDEXED_B <= 'z'))");
    // FIELD_H is indexed, but a range only reaches the visitor outside a bounded range marker when it is open ended, and an open ended range cannot be scanned
    private static final Term OPEN_RANGE = Term.unresolved("FIELD_H >= 'value-h'");

    private static final Term DELAYED = Term.marked("((_Delayed_ = true) && (NON_INDEXED_C == 'value-m'))");
    private static final Term EVAL_ONLY = Term.marked("((_Eval_ = true) && (NON_INDEXED_D =~ 'value-n.*'))");
    // @formatter:on

    /** every term, used for the one and two slot forms */
    private static final List<Term> ALL_TERMS = List.of(EQ_A, EQ_B, EQ_C, REGEX, RANGE, LIST, VALUE, LENIENT, NON_INDEXED, FUNCTION, NON_INDEXED_RANGE,
                    OPEN_RANGE, DELAYED, EVAL_ONLY);

    /**
     * One term per distinct visitor code path, used for the three slot forms to keep the sweep to a few thousand combinations. A term dropped here is handled
     * by the same code path as one that is retained, and the two slot forms still cover every term. Two markers are kept rather than one so that a shape
     * pairing two of them, which slots cannot fill from a single term, is still reachable.
     */
    private static final List<Term> CORE_TERMS = List.of(EQ_A, EQ_B, REGEX, RANGE, LIST, LENIENT, NON_INDEXED, FUNCTION, OPEN_RANGE, DELAYED, EVAL_ONLY);

    @BeforeEach
    public void setup() {
        clearState();
        writeFixture();
    }

    /**
     * Writes one field per resolvable term, with deliberately interleaved uids so unions, intersections and differences are all non-trivial.
     */
    private void writeFixture() {
        writeIndices("FIELD_A", "value-a", 1, 2, 3, 4, 5, 6, 7, 8);
        writeIndices("FIELD_B", "value-b", 5, 6, 7, 8, 9, 10, 11, 12);
        writeIndices("FIELD_C", "value-c", 2, 4, 6, 8, 10, 12);

        // the regex must discriminate, so write a value it does not match
        writeIndices("FIELD_D", "match-one", 1, 3, 5);
        writeIndices("FIELD_D", "match-two", 7, 9, 11);
        writeIndices("FIELD_D", "other-one", 2, 4);

        // values 'b' through 'd' fall in the bounded range, 'a' and 'e' do not
        writeIndices("FIELD_E", "a", 1, 2);
        writeIndices("FIELD_E", "b", 3, 4);
        writeIndices("FIELD_E", "c", 5, 6);
        writeIndices("FIELD_E", "d", 7, 8);
        writeIndices("FIELD_E", "e", 9, 10);

        // the list marker names 'one' and 'three', not 'two'
        writeIndices("FIELD_F", "one", 1, 2, 3);
        writeIndices("FIELD_F", "two", 4, 5, 6);
        writeIndices("FIELD_F", "three", 7, 8, 9);

        writeIndices("FIELD_G", "gv-1", 2, 3, 4);
        writeIndices("FIELD_G", "gv-2", 9, 10);
        writeIndices("FIELD_G", "other", 11, 12);

        writeIndices("FIELD_I", "value-i", 1, 4, 5, 8, 9, 12);

        // FIELD_H is indexed but only ever appears in an open ended range the visitor refuses before reading anything, FIELD_J is indexed but only ever
        // appears inside a function the visitor cannot resolve, and the NON_INDEXED_* fields are not indexed at all, so none of them gets index data. All are
        // opaque to the visitor by construction
    }

    @Test
    public void testSingleTerms() {
        sweep("single term", new ArrayList<>(slots(ALL_TERMS)));
    }

    @Test
    public void testUnions() {
        List<ModeledQuery> models = new ArrayList<>();
        for (ModeledQuery left : slots(ALL_TERMS)) {
            for (ModeledQuery right : slots(ALL_TERMS)) {
                if (distinct(left, right)) {
                    models.add(ModeledQuery.or(left, right));
                }
            }
        }
        sweep("union", models);
    }

    @Test
    public void testIntersections() {
        List<ModeledQuery> models = new ArrayList<>();
        for (ModeledQuery left : slots(ALL_TERMS)) {
            for (ModeledQuery right : slots(ALL_TERMS)) {
                if (distinct(left, right)) {
                    models.add(ModeledQuery.and(left, right));
                }
            }
        }
        sweep("intersection", models);
    }

    @Test
    public void testNestedUnions() {
        List<ModeledQuery> models = new ArrayList<>();
        for (ModeledQuery anchor : slots(CORE_TERMS)) {
            for (ModeledQuery left : slots(CORE_TERMS)) {
                for (ModeledQuery right : slots(CORE_TERMS)) {
                    if (distinct(anchor, left, right)) {
                        models.add(ModeledQuery.and(anchor, ModeledQuery.or(left, right)));
                    }
                }
            }
        }
        sweep("nested union", models);
    }

    @Test
    public void testNestedIntersections() {
        List<ModeledQuery> models = new ArrayList<>();
        for (ModeledQuery anchor : slots(CORE_TERMS)) {
            for (ModeledQuery left : slots(CORE_TERMS)) {
                for (ModeledQuery right : slots(CORE_TERMS)) {
                    if (distinct(anchor, left, right)) {
                        models.add(ModeledQuery.or(anchor, ModeledQuery.and(left, right)));
                    }
                }
            }
        }
        sweep("nested intersection", models);
    }

    /**
     * Every term in both polarities
     *
     * @param terms
     *            the term catalogue
     * @return one model per term per polarity
     */
    private List<ModeledQuery> slots(List<Term> terms) {
        List<ModeledQuery> slots = new ArrayList<>();
        for (Term term : terms) {
            slots.add(ModeledQuery.term(term));
            slots.add(ModeledQuery.negated(term));
        }
        return slots;
    }

    /**
     * Whether every slot uses a different field and value.
     * <p>
     * Repeating a term across slots builds a query no planner would emit and no user would write, and one whose answer is not even well defined: in
     * {@code A == 'a' && (B == 'b' || A != 'a')} the truth of the second disjunct is already settled by the anchor, and for a multi valued field it is settled
     * ambiguously. Terms in different slots must be independent for the three valued model to mean anything.
     *
     * @param slots
     *            the slots of one combination
     * @return true when no term is repeated
     */
    private boolean distinct(ModeledQuery... slots) {
        Set<Term> seen = new LinkedHashSet<>();
        for (ModeledQuery slot : slots) {
            for (Term term : slot.terms()) {
                if (!seen.add(term)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Drives every model, groups the results by shape, and compares the misbehaving shapes against the recorded gaps
     *
     * @param form
     *            the name of the query form, used in failure messages
     * @param models
     *            every combination of the form
     */
    private void sweep(String form, List<ModeledQuery> models) {
        Map<String,Shape> shapes = new TreeMap<>();

        for (ModeledQuery model : models) {
            Shape shape = shapes.computeIfAbsent(model.shape(), Shape::new);
            shape.total++;

            String unreachable = unreachable(model.query());
            if (unreachable != null) {
                shape.unreachable++;
                if (shape.unreachableExample == null) {
                    shape.unreachableExample = model.query() + "\n      " + unreachable;
                }
                continue;
            }

            ScanResult result;
            try {
                result = drive(model.query());
            } catch (Exception e) {
                throw new IllegalStateException("visitor threw for query: " + model.query(), e);
            }

            boolean modelBounded = model.bound() != null;
            boolean visitorBounded = result != null && result.isBounded();
            if (modelBounded != visitorBounded) {
                shape.recordBoundMismatch(model, modelBounded);
                continue;
            }

            if (!modelBounded) {
                // the field index owes a refusal, not a set, so there is nothing more to compare
                shape.unbounded++;
                continue;
            }

            Set<Integer> expected = model.candidates(UNIVERSE);
            Set<Integer> actual = uids(result);

            if (!actual.containsAll(expected)) {
                shape.recordDropped(model, expected, actual);
            }
            if (!expected.containsAll(actual)) {
                shape.recordExtra(model, expected, actual);
            }
        }

        report(form, shapes);
    }

    /** the results for a single query shape */
    private static final class Shape {
        private final String shape;
        private int total;
        private int unbounded;
        private int unreachable;
        private String unreachableExample;
        private int dropped;
        private int extra;
        private int boundMismatch;
        private String droppedExample;
        private String extraExample;
        private String boundMismatchExample;

        private Shape(String shape) {
            this.shape = shape;
        }

        private void recordDropped(ModeledQuery model, Set<Integer> expected, Set<Integer> actual) {
            dropped++;
            if (droppedExample == null) {
                droppedExample = example(model, expected, actual);
            }
        }

        private void recordExtra(ModeledQuery model, Set<Integer> expected, Set<Integer> actual) {
            extra++;
            if (extraExample == null) {
                extraExample = example(model, expected, actual);
            }
        }

        private void recordBoundMismatch(ModeledQuery model, boolean modelBounded) {
            boundMismatch++;
            if (boundMismatchExample == null) {
                // @formatter:off
                boundMismatchExample = "\n      query:    " + model.query()
                                + "\n      expected: " + (modelBounded ? "a bound" : "no bound")
                                + "\n      actual:   " + (modelBounded ? "no bound" : "a bound");
                // @formatter:on
            }
        }

        private String example(ModeledQuery model, Set<Integer> expected, Set<Integer> actual) {
            return "\n      query:    " + model.query() + "\n      expected: " + expected + "\n      actual:   " + actual;
        }
    }

    /**
     * Fails with a per shape summary when an unrecorded shape misbehaves, or when a recorded gap no longer does
     *
     * @param form
     *            the name of the query form
     * @param shapes
     *            the results, keyed by shape
     */
    private void report(String form, Map<String,Shape> shapes) {
        Set<String> drops = new LinkedHashSet<>();
        Set<String> extras = new LinkedHashSet<>();
        StringBuilder regressions = new StringBuilder();
        int total = 0;
        int unbounded = 0;

        StringBuilder stale = new StringBuilder();
        int reachable = 0;
        for (Shape shape : shapes.values()) {
            total += shape.total;
            unbounded += shape.unbounded;
            reachable += shape.total - shape.unreachable;

            if (shape.boundMismatch > 0) {
                // never an accepted gap: the visitor must agree with the model on whether the query can be answered at all
                append(regressions, shape, "disagrees with the model on whether the query can be bounded", shape.boundMismatch, shape.boundMismatchExample);
            }

            if (shape.dropped > 0) {
                drops.add(shape.shape);
                if (!DROPS_CANDIDATES.contains(shape.shape)) {
                    append(regressions, shape, "drops candidates the query can match", shape.dropped, shape.droppedExample);
                }
            }

            if (shape.extra > 0) {
                extras.add(shape.shape);
                if (!RETURNS_EXTRA_CANDIDATES.contains(shape.shape)) {
                    append(regressions, shape, "returns candidates the query cannot match", shape.extra, shape.extraExample);
                }
            }

            boolean listed = DROPS_CANDIDATES.contains(shape.shape) || RETURNS_EXTRA_CANDIDATES.contains(shape.shape);
            if (listed && shape.total - shape.unreachable == 0) {
                stale.append("\n  ").append(shape.shape).append(", no combination of it reaches the visitor any more");
            }
        }

        if (reachable == 0) {
            fail(form + ": every combination was filtered out as unreachable, the term catalogue no longer exercises this form");
        }

        StringBuilder fixed = new StringBuilder();
        collectFixed(fixed, shapes, DROPS_CANDIDATES, drops, "DROPS_CANDIDATES");
        collectFixed(fixed, shapes, RETURNS_EXTRA_CANDIDATES, extras, "RETURNS_EXTRA_CANDIDATES");

        if (regressions.length() == 0 && fixed.length() == 0 && stale.length() == 0) {
            return;
        }

        // @formatter:off
        StringBuilder failure = new StringBuilder()
                        .append(form).append(": ").append(reachable).append(" of ").append(total).append(" combinations reached the visitor, across ")
                        .append(shapes.size()).append(" shapes (").append(unbounded).append(" not answerable from the index)");
        // @formatter:on

        if (regressions.length() > 0) {
            failure.append("\n\nshapes that misbehave and are not yet recorded as a gap:").append(regressions);
        }
        if (fixed.length() > 0) {
            failure.append("\n\nshapes recorded as a gap that now behave, remove them from the list:").append(fixed);
        }
        if (stale.length() > 0) {
            failure.append("\n\nshapes recorded as a gap that are no longer covered, the list is stale:").append(stale);
        }
        fail(failure.toString());
    }

    private void append(StringBuilder regressions, Shape shape, String problem, int count, String example) {
        // @formatter:off
        regressions.append("\n  ").append(shape.shape).append("  ").append(problem)
                        .append(" (").append(count).append(" of ").append(shape.total).append(" combinations)")
                        .append(example);
        // @formatter:on
    }

    private void collectFixed(StringBuilder fixed, Map<String,Shape> shapes, Set<String> gaps, Set<String> failing, String list) {
        for (String gap : gaps) {
            if (shapes.containsKey(gap) && !failing.contains(gap)) {
                fixed.append("\n  ").append(gap).append(" in ").append(list);
            }
        }
    }

    /**
     * Why a query could never reach the visitor, or null when it could.
     * <p>
     * The query planner only hands the field index queries it has already determined are executable, and it pushes negations down first, so a query the
     * pushdown rewrites is not the form the visitor would ever be given. Sweeping either kind would be testing the visitor against an input it cannot see.
     *
     * @param query
     *            the query
     * @return the reason the query is unreachable, or null
     */
    private String unreachable(String query) {
        ASTJexlScript script = parse(query);

        String pushedDown = JexlStringBuildingVisitor.buildQuery(PushdownNegationVisitor.pushdownNegations(script));
        String flattened = JexlStringBuildingVisitor.buildQuery(TreeFlatteningRebuildingVisitor.flatten(parse(query)));
        if (!flattened.equals(pushedDown)) {
            // not a filter but a bug in the term catalogue: a term whose negation the pushdown rewrites is not a distinct input, and
            // the positive form of whatever it was rewritten into is already swept
            throw new IllegalStateException("the negation pushdown rewrites [" + flattened + "] into [" + pushedDown + "], so it is not an input the visitor"
                            + " can ever be given. The term catalogue should not generate it.");
        }

        // the field sets are supplied directly, so no MetadataHelper is needed
        // @formatter:off
        STATE state = ExecutableDeterminationVisitor.getState(script, new ShardQueryConfiguration(), INDEXED_FIELDS, Set.of(), Set.of(),
                        true, null, null);
        // @formatter:on
        return state == STATE.EXECUTABLE ? null : "the planner reports " + state;
    }

    /**
     * Runs the visitor over the fixture
     *
     * @param query
     *            the query
     * @return the bound the visitor produced, or null when it could not bound the query
     */
    private ScanResult drive(String query) {
        ASTJexlScript script = parse(query);
        SortedKeyValueIterator<Key,Value> source = createSource();

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypes, null, INDEXED_FIELDS);
        return visitor.getScanResult(script);
    }

    /**
     * Extracts the uids from a bound
     *
     * @param result
     *            a positive ScanResult
     * @return the candidate uids
     */
    private Set<Integer> uids(ScanResult result) {
        Set<Integer> uids = new TreeSet<>();
        for (Key id : result.getResults()) {
            String cf = id.getColumnFamily().toString();
            uids.add(Integer.parseInt(cf.substring(cf.lastIndexOf('-') + 1)) - 1_000);
        }
        return uids;
    }

    @Override
    protected BaseDocIdIterator createIterator() {
        throw new IllegalStateException("Should never be called");
    }
}

package datawave.next;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * A query form together with the answer the field index owes for it, used to derive test expectations instead of hand writing them.
 * <p>
 * A term the field index cannot resolve is UNKNOWN rather than false, so a document is a candidate unless the query is definitively false for it. Separately,
 * {@link #bound()} says whether the index can name the candidates at all: it cannot enumerate the documents a negation matches, so a query that reduces to one
 * is unbounded, and {@link DocIdIteratorVisitor} owes a refusal rather than a set.
 */
public abstract class ModeledQuery {

    /** three valued logic, where UNKNOWN is a term the field index cannot decide */
    public enum Truth {
        TRUE, FALSE, UNKNOWN;

        Truth negate() {
            switch (this) {
                case TRUE:
                    return FALSE;
                case FALSE:
                    return TRUE;
                default:
                    return UNKNOWN;
            }
        }

        static Truth and(Truth left, Truth right) {
            if (left == FALSE || right == FALSE) {
                return FALSE;
            }
            return (left == UNKNOWN || right == UNKNOWN) ? UNKNOWN : TRUE;
        }

        static Truth or(Truth left, Truth right) {
            if (left == TRUE || right == TRUE) {
                return TRUE;
            }
            return (left == UNKNOWN || right == UNKNOWN) ? UNKNOWN : FALSE;
        }
    }

    /** how the visitor handles a term, which drives the shape notation used to group results */
    public enum Resolution {
        /** resolved against the field index */
        RESOLVED("R"),
        /** cannot be resolved */
        UNRESOLVED("U"),
        /** cannot be resolved because a marker makes its source non-executable, kept separate since it is a different shape of input */
        MARKED("M");

        private final String symbol;

        Resolution(String symbol) {
            this.symbol = symbol;
        }
    }

    /** a single query term and the uids it matches in the fixture */
    public static final class Term {
        private final String query;
        private final Resolution resolution;
        private final Set<Integer> uids;

        private Term(String query, Resolution resolution, Set<Integer> uids) {
            this.query = query;
            this.resolution = resolution;
            this.uids = uids;
        }

        public static Term resolved(String query, Set<Integer> uids) {
            return new Term(query, Resolution.RESOLVED, uids);
        }

        public static Term resolved(String query, Integer... uids) {
            return new Term(query, Resolution.RESOLVED, Set.of(uids));
        }

        public static Term unresolved(String query) {
            return new Term(query, Resolution.UNRESOLVED, Set.of());
        }

        public static Term marked(String query) {
            return new Term(query, Resolution.MARKED, Set.of());
        }

        public boolean isResolved() {
            return resolution == Resolution.RESOLVED;
        }
    }

    /** the query text, which is what gets parsed and handed to the visitor */
    public abstract String query();

    /** the truth of this expression for a single uid */
    public abstract Truth eval(int uid);

    /**
     * The uids the field index can bound this expression to, or null when it cannot bound it at all. Unbounded is not the same as empty: the visitor owes a
     * refusal, since there is no way to enumerate every document that does not match a term.
     *
     * @return the bound, or null
     */
    public abstract Set<Integer> bound();

    /** the shape of this expression, naming how the visitor handles each slot */
    public abstract String shape();

    /** every term this expression uses, for checking that a combination does not repeat one */
    public abstract Set<Term> terms();

    /**
     * The candidates the visitor should produce, which is every uid the query is not definitively false for
     *
     * @param universe
     *            every uid in the fixture
     * @return the expected candidate uids
     */
    public Set<Integer> candidates(Set<Integer> universe) {
        Set<Integer> candidates = new TreeSet<>();
        for (Integer uid : new TreeSet<>(universe)) {
            if (eval(uid) != Truth.FALSE) {
                candidates.add(uid);
            }
        }
        return candidates;
    }

    public static ModeledQuery term(Term term) {
        return new TermQuery(term, false);
    }

    public static ModeledQuery negated(Term term) {
        return new TermQuery(term, true);
    }

    public static ModeledQuery and(ModeledQuery... children) {
        return new JunctionQuery(true, Arrays.asList(children));
    }

    public static ModeledQuery or(ModeledQuery... children) {
        return new JunctionQuery(false, Arrays.asList(children));
    }

    private static final class TermQuery extends ModeledQuery {
        private final Term term;
        private final boolean negatedTerm;

        private TermQuery(Term term, boolean negatedTerm) {
            this.term = term;
            this.negatedTerm = negatedTerm;
        }

        @Override
        public String query() {
            return negatedTerm ? "!(" + term.query + ")" : term.query;
        }

        @Override
        public Truth eval(int uid) {
            Truth truth = term.isResolved() ? (term.uids.contains(uid) ? Truth.TRUE : Truth.FALSE) : Truth.UNKNOWN;
            return negatedTerm ? truth.negate() : truth;
        }

        @Override
        public Set<Integer> bound() {
            // the index cannot enumerate the documents a negation matches
            return (term.isResolved() && !negatedTerm) ? term.uids : null;
        }

        @Override
        public String shape() {
            return (negatedTerm ? "!" : "") + term.resolution.symbol;
        }

        @Override
        public Set<Term> terms() {
            return Set.of(term);
        }
    }

    private static final class JunctionQuery extends ModeledQuery {
        private final boolean intersection;
        private final List<ModeledQuery> children;

        private JunctionQuery(boolean intersection, List<ModeledQuery> children) {
            this.intersection = intersection;
            this.children = children;
        }

        private String operator() {
            return intersection ? " && " : " || ";
        }

        @Override
        public String query() {
            List<String> parts = new ArrayList<>();
            for (ModeledQuery child : children) {
                parts.add(child.query());
            }
            return "(" + String.join(operator(), parts) + ")";
        }

        @Override
        public Truth eval(int uid) {
            Truth truth = null;
            for (ModeledQuery child : children) {
                Truth childTruth = child.eval(uid);
                truth = truth == null ? childTruth : (intersection ? Truth.and(truth, childTruth) : Truth.or(truth, childTruth));
            }
            return truth;
        }

        @Override
        public Set<Integer> bound() {
            Set<Integer> bound = null;
            for (ModeledQuery child : children) {
                Set<Integer> childBound = child.bound();

                if (childBound == null) {
                    if (intersection) {
                        // an unbounded operand narrows nothing, so the bound so far still holds
                        continue;
                    }
                    // an unbounded disjunct could be true for any document
                    return null;
                }

                if (bound == null) {
                    bound = new TreeSet<>(childBound);
                } else if (intersection) {
                    bound.retainAll(childBound);
                } else {
                    bound.addAll(childBound);
                }
            }
            return bound;
        }

        @Override
        public String shape() {
            List<String> parts = new ArrayList<>();
            for (ModeledQuery child : children) {
                parts.add(child.shape());
            }
            return "(" + String.join(operator(), parts) + ")";
        }

        @Override
        public Set<Term> terms() {
            Set<Term> terms = new LinkedHashSet<>();
            for (ModeledQuery child : children) {
                terms.addAll(child.terms());
            }
            return terms;
        }
    }
}

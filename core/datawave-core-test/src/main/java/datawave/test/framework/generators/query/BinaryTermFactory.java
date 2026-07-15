package datawave.test.framework.generators.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.term.QueryTerm;

/**
 * Base class for {@link QueryMetadataFactory} implementations that combine two {@link QueryTerm} instances into a single query.
 * <p>
 * Subclasses provide the combining operator (e.g. {@code &&} or {@code ||}) and the id-set combination logic (e.g. intersection or union), and validate the
 * terms' negation state. Each term only tracks the ids relevant to its own field; it is up to the combination logic to interpret a negated term's ids in
 * context (e.g. an intersection with a negated term is a set difference, not an intersection of complements).
 */
public abstract class BinaryTermFactory implements QueryMetadataFactory {

    protected final QueryTerm leftTerm;
    protected final QueryTerm rightTerm;

    protected List<FieldMetadata> leftMetadata;
    protected List<FieldMetadata> rightMetadata;

    protected BinaryTermFactory(QueryTerm leftTerm, QueryTerm rightTerm) {
        this.leftTerm = leftTerm;
        this.rightTerm = rightTerm;
    }

    public void setLeftMetadata(List<FieldMetadata> leftMetadata) {
        this.leftMetadata = leftMetadata;
    }

    public void setRightMetadata(List<FieldMetadata> rightMetadata) {
        this.rightMetadata = rightMetadata;
    }

    /**
     * The Jexl operator used to join the left and right terms, e.g. {@code &&} or {@code ||}
     */
    protected abstract String operator();

    /**
     * Combine the left and right term's expected event ids, given each term's negation state.
     */
    protected abstract Set<Integer> combine(Set<Integer> leftIds, boolean leftNegated, Set<Integer> rightIds, boolean rightNegated);

    /**
     * Validate the negation state of the two terms. Throws {@link IllegalStateException} if the combination is not supported.
     */
    protected abstract void validateTerms(QueryTerm leftTerm, QueryTerm rightTerm);

    @Override
    public final List<QueryMetadata> getQueries() {
        validateTerms(leftTerm, rightTerm);

        List<QueryMetadata> metadata = new ArrayList<>();
        boolean selfComposed = leftMetadata.equals(rightMetadata);

        for (int i = 0; i < leftMetadata.size(); i++) {
            FieldMetadata left = leftMetadata.get(i);
            leftTerm.givenFieldMetadata(left);

            for (String leftValue : leftTerm.valuesFor(left)) {
                leftTerm.givenValue(leftValue);
                List<Integer> leftIds = leftTerm.getEventIds();

                int startJ = selfComposed ? i + 1 : 0;
                for (int j = startJ; j < rightMetadata.size(); j++) {
                    FieldMetadata right = rightMetadata.get(j);
                    rightTerm.givenFieldMetadata(right);

                    for (String rightValue : rightTerm.valuesFor(right)) {
                        rightTerm.givenValue(rightValue);
                        List<Integer> rightIds = rightTerm.getEventIds();

                        String query = leftTerm.getQueryTerm() + " " + operator() + " " + rightTerm.getQueryTerm();
                        String plan;
                        if (leftTerm.getPlanTerm().equals(rightTerm.getPlanTerm())) {
                            plan = leftTerm.getPlanTerm(); // terms are deduplicated in query planning
                        } else {
                            plan = leftTerm.getPlanTerm() + " " + operator() + " " + rightTerm.getPlanTerm();
                        }

                        Set<Integer> ids = combine(new HashSet<>(leftIds), leftTerm.isNegated(), new HashSet<>(rightIds), rightTerm.isNegated());
                        // combine() treats leftIds/rightIds as immutable and returns a fresh result set
                        metadata.add(QueryMetadata.of(query, plan, new ArrayList<>(ids)));
                    }
                }
            }
        }
        return metadata;
    }
}

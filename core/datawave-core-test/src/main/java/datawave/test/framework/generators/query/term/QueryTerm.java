package datawave.test.framework.generators.query.term;

import java.util.List;

import datawave.test.framework.FieldMetadata;

/**
 * Produces query text, plan text, and event ids for a term applied to field metadata.
 * <p>
 * {@link #forEachEvaluation(FieldMetadata, Runnable)} invokes the callback after each application. The callback can read the current query text, plan text, and
 * event ids through the getters; subsequent applications may replace them.
 */
public interface QueryTerm {

    /**
     * Applies this term to the given field once for every evaluation the field requires, running {@code onEvaluation} after each application so the caller can
     * read the resulting {@link #getQueryTerm()}, {@link #getPlanTerm()} and {@link #getEventIds()}.
     * <p>
     * How many evaluations a field requires depends on the term: a {@link ValueDependentQueryTerm} is applied once per {@link FieldMetadata#getValues()} entry,
     * while a {@link ValueIndependentQueryTerm} is applied exactly once per field because its text does not name a value.
     *
     * @param fieldMetadata
     *            the field to apply this term to
     * @param onEvaluation
     *            run once after each application of this term
     */
    void forEachEvaluation(FieldMetadata fieldMetadata, Runnable onEvaluation);

    List<String> createNormalizedValues(String value);

    String getQueryTerm();

    String getPlanTerm();

    List<Integer> getEventIds();

    boolean isNegated();

    void setIsNegated(boolean isNegated);
}

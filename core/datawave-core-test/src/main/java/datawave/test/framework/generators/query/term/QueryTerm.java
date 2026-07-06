package datawave.test.framework.generators.query.term;

import java.util.List;

import datawave.test.framework.FieldMetadata;
import datawave.test.framework.generators.query.QueryMetadata;

/**
 * Encapsulates logic for building a portion of the {@link QueryMetadata}
 * <p>
 * Some terms cannot be evaluated to a correct, final event id set using only their own {@link FieldMetadata}; implementations should document this on the class
 * itself. Two categories require such external context:
 * <ul>
 * <li><b>Negated terms</b> ({@link #isNegated()} returns {@code true}, e.g. {@code NeTerm}, {@code IsNullTerm}): the ids tracked by the term are only
 * meaningful once interpreted by the combining {@code QueryMetadataFactory} alongside a partner term (e.g. as a set difference in an intersection). A negated
 * term's ids should never be treated as a standalone, final result set.</li>
 * <li><b>Filter functions</b> (e.g. {@code filter:isNotNull}, {@code filter:isNull}): these are Jexl filter functions evaluated during the evaluation phase,
 * not the index lookup phase, so they cannot anchor a query on their own. They must always be paired with at least one non-filter, non-negated term elsewhere
 * in the query to provide an indexed anchor.</li>
 * </ul>
 */
public interface QueryTerm {

    void givenFieldMetadata(FieldMetadata fieldMetadata);

    void givenValue(String value);

    List<String> createNormalizedValues(String value);

    /**
     * The values this term should be evaluated against for the given field metadata.
     * <p>
     * Most terms are value-dependent and simply iterate over {@link FieldMetadata#getValues()}. Terms whose query/plan text does not depend on a specific value
     * (e.g. {@code isNotNull}) should override this to return a single-element list so callers evaluate the term exactly once per field instead of once per
     * value.
     *
     * @param fieldMetadata
     *            the field metadata
     * @return the values to evaluate this term against
     */
    default List<String> valuesFor(FieldMetadata fieldMetadata) {
        return fieldMetadata.getValues();
    }

    String getQueryTerm();

    String getPlanTerm();

    List<Integer> getEventIds();

    boolean isNegated();

    void setIsNegated(boolean isNegated);
}

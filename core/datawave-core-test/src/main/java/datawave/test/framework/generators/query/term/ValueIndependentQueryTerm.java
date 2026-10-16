package datawave.test.framework.generators.query.term;

import datawave.test.framework.FieldMetadata;

/**
 * Base class for {@link QueryTerm} implementations whose query and plan text name only a field, never a value, e.g. the {@code filter:isNull} and
 * {@code filter:isNotNull} filter functions.
 * <p>
 * Such a term is applied exactly once per field rather than once per value, and is never handed a value - real or placeholder - to ignore.
 */
public abstract class ValueIndependentQueryTerm extends AbstractQueryTerm {

    @Override
    public final void forEachEvaluation(FieldMetadata fieldMetadata, Runnable onEvaluation) {
        this.metadata = fieldMetadata;
        apply();
        onEvaluation.run();
    }

    /**
     * Builds this term's query text, plan text and event ids against the current field.
     */
    protected abstract void apply();
}

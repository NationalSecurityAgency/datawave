package datawave.test.framework.generators.query.term;

import datawave.test.framework.FieldMetadata;

/**
 * Base class for {@link QueryTerm} implementations whose query and plan text name a specific field value, e.g. {@code FIELD == 'value'}.
 * <p>
 * Such a term is applied once per value the field carries, so every application receives a real value drawn from {@link FieldMetadata#getValues()}.
 */
public abstract class ValueDependentQueryTerm extends AbstractQueryTerm {

    @Override
    public final void forEachEvaluation(FieldMetadata fieldMetadata, Runnable onEvaluation) {
        this.metadata = fieldMetadata;
        for (String value : fieldMetadata.getValues()) {
            apply(value);
            onEvaluation.run();
        }
    }

    /**
     * Builds this term's query text, plan text and event ids against a single value of the current field.
     *
     * @param value
     *            the value to build this term against
     */
    protected abstract void apply(String value);
}

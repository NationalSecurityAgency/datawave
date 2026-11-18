package datawave.core.query.logic;

import datawave.marking.MarkingFunctions;

public abstract class BaseQueryLogicTransformer<I,O> extends AbstractQueryLogicTransformer<I,O> implements QueryLogicTransformer<I,O> {

    /**
     * @deprecated transformers no longer use {@link MarkingFunctions}; response objects carry column visibility strings instead.
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    protected MarkingFunctions<?> markingFunctions;

    public BaseQueryLogicTransformer() {}

    /**
     * @param markingFunctions
     *            ignored
     * @deprecated use {@link #BaseQueryLogicTransformer()}
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    public BaseQueryLogicTransformer(MarkingFunctions<?> markingFunctions) {
        this();
        this.markingFunctions = markingFunctions;
    }
}

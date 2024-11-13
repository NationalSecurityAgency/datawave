package datawave.core.query.logic;

import datawave.marking.MarkingFunctions;

public abstract class BaseQueryLogicTransformer<I,O> extends AbstractQueryLogicTransformer<I,O> implements QueryLogicTransformer<I,O> {

    protected MarkingFunctions markingFunctions;

    public BaseQueryLogicTransformer(MarkingFunctions markingFunctions) {
        if (null == markingFunctions) {
            throw new IllegalArgumentException("MarkingFunctions must be set");
        }
        this.markingFunctions = markingFunctions;
    }
}

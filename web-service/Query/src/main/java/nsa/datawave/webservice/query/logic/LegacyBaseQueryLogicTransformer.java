package nsa.datawave.webservice.query.logic;

import nsa.datawave.marking.MarkingFunctions;

/**
 * @deprecated as of version3.5.0, replaced by refactored BaseQueryLogicTransformer
 */
@Deprecated
public abstract class LegacyBaseQueryLogicTransformer extends AbstractQueryLogicTransformer implements QueryLogicTransformer {
    protected MarkingFunctions markingFunctions = null;
    
    public LegacyBaseQueryLogicTransformer(MarkingFunctions markingFunctions) {
        if (null == markingFunctions) {
            throw new IllegalArgumentException("MarkingFunctions must be set");
        }
        this.markingFunctions = markingFunctions;
    }
}

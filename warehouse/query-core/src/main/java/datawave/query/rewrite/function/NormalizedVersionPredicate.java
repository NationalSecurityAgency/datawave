package datawave.query.rewrite.function;

import datawave.query.rewrite.predicate.ConfiguredPredicate;

/**
 * Predicate implementation of the AbstractVersionFilter configured for use as filters by the "BaseEventQuery" RefactoredShardQueryLogic and "TLDEventQuery"
 * RefactoredTLDQueryLogic.
 *
 * @param <A>
 * 
 * @see AbstractVersionFilter
 * @see datawave.query.rewrite.tables.RefactoredShardQueryLogic
 * @see datawave.query.rewrite.tables.RefactoredTLDQueryLogic
 */
public class NormalizedVersionPredicate<A> extends AbstractVersionFilter<A> implements ConfiguredPredicate<A> {
    @Override
    public boolean apply(final A input) {
        final A output = this.apply(input, true);
        return !(null == output);
    }
}

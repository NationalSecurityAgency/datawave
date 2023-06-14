package datawave.query.function;

import datawave.query.predicate.ConfiguredPredicate;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;

/**
 * Predicate implementation of the AbstractVersionFilter configured for use as filters by the "BaseEventQuery" ShardQueryLogic and "TLDEventQuery"
 * TLDQueryLogic.
 *
 * @param <A>
 *            type for the predicate
 *
 * @see AbstractVersionFilter
 * @see ShardQueryLogic
 * @see TLDQueryLogic
 */
public class NormalizedVersionPredicate<A> extends AbstractVersionFilter<A> implements ConfiguredPredicate<A> {
    @Override
    public boolean apply(final A input) {
        final A output = this.apply(input, true);
        return !(null == output);
    }
}

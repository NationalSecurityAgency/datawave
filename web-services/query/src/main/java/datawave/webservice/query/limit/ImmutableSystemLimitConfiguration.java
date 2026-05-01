package datawave.webservice.query.limit;

import java.util.Map;

/**
 * An immutable implementation of {@link SystemLimitConfiguration} that prevents modifications and uses immutable internal members.
 */
public final class ImmutableSystemLimitConfiguration extends SystemLimitConfiguration {

    /**
     * Return an immutable copy of the given {@link SystemLimitConfiguration}.
     *
     * @param config
     *            the config
     */
    public ImmutableSystemLimitConfiguration(SystemLimitConfiguration config) {
        super.setSystemPattern(config.getSystemPattern());
        super.setCountsAgainstUserLimit(config.getCountsAgainstUserLimit());
        super.setQueryLimit(config.getQueryLimit());
        super.setQueryLogicGroupLimits(config.getQueryLogicGroupLimits() == null ? null : Map.copyOf(config.getQueryLogicGroupLimits()));
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setSystemPattern(String systemPattern) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setCountsAgainstUserLimit(Boolean countsAgainstUserLimit) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setQueryLimit(Integer queryLimit) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setQueryLogicGroupLimits(Map<String,Integer> queryLogicGroupLimits) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return toString(ImmutableSystemLimitConfiguration.class);
    }
}

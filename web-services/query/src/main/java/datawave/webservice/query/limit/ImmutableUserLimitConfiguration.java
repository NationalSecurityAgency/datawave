package datawave.webservice.query.limit;

import java.util.Map;

/**
 * An immutable implementation of {@link UserLimitConfiguration} that prevents modifications and uses immutable internal members.
 */
public final class ImmutableUserLimitConfiguration extends UserLimitConfiguration {

    /**
     * Return an immutable copy of the given {@link UserLimitConfiguration}.
     *
     * @param config
     *            the config
     */
    public ImmutableUserLimitConfiguration(UserLimitConfiguration config) {
        super.setUserDn(config.getUserDn());
        super.setQueryLimit(config.getQueryLimit());
        super.setQueryLogicGroupLimits(config.getQueryLogicGroupLimits() == null ? Map.of() : Map.copyOf(config.getQueryLogicGroupLimits()));
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setUserDn(String userDn) {
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
        return super.toString(ImmutableUserLimitConfiguration.class);
    }

}

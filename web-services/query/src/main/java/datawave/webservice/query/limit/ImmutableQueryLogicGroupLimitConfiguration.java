package datawave.webservice.query.limit;

/**
 * An immutable implementation of {@link QueryLogicGroupLimitConfiguration} that prevents modifications and uses immutable internal members.
 */
public final class ImmutableQueryLogicGroupLimitConfiguration extends QueryLogicGroupLimitConfiguration {

    /**
     * Return an immutable copy of the given {@link QueryLogicGroupLimitConfiguration}.
     *
     * @param config
     *            the config
     */
    public ImmutableQueryLogicGroupLimitConfiguration(QueryLogicGroupLimitConfiguration config) {
        super.setGroupName(config.getGroupName());
        super.setQueryLogicPattern(config.getQueryLogicPattern());
        super.setQueryLimit(config.getQueryLimit());
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setGroupName(String groupName) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setQueryLogicPattern(String queryLogicPattern) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setQueryLimit(int queryLimit) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return toString(ImmutableQueryLogicGroupLimitConfiguration.class);
    }
}

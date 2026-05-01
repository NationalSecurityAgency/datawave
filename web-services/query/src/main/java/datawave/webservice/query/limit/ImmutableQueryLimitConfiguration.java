package datawave.webservice.query.limit;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An immutable implementation of {@link QueryLimitConfiguration} that prevents modifications and uses immutable internal members.
 */
public final class ImmutableQueryLimitConfiguration extends QueryLimitConfiguration {

    /**
     * Return an immutable copy of the given {@link QueryLimitConfiguration}.
     *
     * @param config
     *            the config
     */
    public ImmutableQueryLimitConfiguration(QueryLimitConfiguration config) {
        super.setDefaultUserQueryLimit(config.getDefaultUserQueryLimit());
        super.setDefaultSystemQueryLimit(config.getDefaultSystemQueryLimit());
        super.setInternalCacheMaxSize(config.getInternalCacheMaxSize());
        super.setUserConfigs(copyList(config.getUserConfigs(), ImmutableUserLimitConfiguration::new));
        super.setSystemConfigs(copyList(config.getSystemConfigs(), ImmutableSystemLimitConfiguration::new));
        super.setQueryLogicGroupConfigs(copyList(config.getQueryLogicGroupConfigs(), ImmutableQueryLogicGroupLimitConfiguration::new));
    }

    /**
     * Return an immutable version of the given list and its elements.
     *
     * @param list
     *            the list to copy
     * @param immutableConstructor
     *            the constructor that will provide an immutable copy of each element
     * @return the immutable list
     * @param <T>
     *            the element type
     */
    private <T> List<T> copyList(List<T> list, Function<T,T> immutableConstructor) {
        if (list == null) {
            return List.of();
        } else {
            return list.stream().map(immutableConstructor).collect(Collectors.toUnmodifiableList());
        }
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setDefaultUserQueryLimit(int defaultUserQueryLimit) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setDefaultSystemQueryLimit(int defaultSystemQueryLimit) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setInternalCacheMaxSize(long internalCacheMaxSize) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setUserConfigs(List<UserLimitConfiguration> userConfigs) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setSystemConfigs(List<SystemLimitConfiguration> systemConfigs) {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setQueryLogicGroupConfigs(List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return toString(ImmutableQueryLimitConfiguration.class);
    }
}

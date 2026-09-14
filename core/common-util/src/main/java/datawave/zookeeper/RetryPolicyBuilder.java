package datawave.zookeeper;

import org.apache.curator.RetryPolicy;

/**
 * Provides functionality for creating {@link RetryPolicy} that can be used when creating {@link org.apache.curator.framework.CuratorFramework} instances for
 * interacting with Zookeeper.
 */
public interface RetryPolicyBuilder<T extends RetryPolicy> {

    /**
     * Create and return a new {@link RetryPolicy}.
     *
     * @return the new policy
     */
    T build();

    /**
     * Return a duplicate of this {@link RetryPolicy}.
     *
     * @return the duplicate
     */
    RetryPolicyBuilder<T> duplicate();
}

package datawave.zookeeper;

import java.util.function.Consumer;

/**
 * A publisher that will, when triggered, load a new instance of a configured
 */
public interface ZkPojoPublisher<T> {

    /**
     * Add a listener that will listen for new publishes of an {@link T} instance from this {@link ZkPojoPublisher}.
     *
     * @param listener
     *            the listener to add
     */
    void addListener(Consumer<T> listener);

    /**
     * Remove a listener.
     *
     * @param listener
     *            the listener to remove
     */
    void removeListener(Consumer<T> listener);
}

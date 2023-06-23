package datawave.core.query.logic;

import datawave.core.query.exception.EmptyObjectException;

public interface Flushable<T> {

    /**
     * The flush method is used to return an results that were cached from the calls to transform(Object). If this method will be called multiple times until a
     * null is returned. If EmptyObjectException is thrown instead of returning null, then flush will be called again.
     *
     * @return A cached object or null if no more exist.
     * @throws EmptyObjectException
     *             if the current cached result is empty, and flush should be called again.
     */
    T flush() throws EmptyObjectException;

}

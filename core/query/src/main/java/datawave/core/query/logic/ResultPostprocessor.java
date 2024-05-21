package datawave.core.query.logic;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import datawave.core.query.configuration.GenericQueryConfiguration;

/**
 * Result Postprocessors are needed by the query microservices for certain query logics which need their results manipulated in some way. An example would be
 * the CountingShardQueryLogic, which needs its events combined into a single event representing the final count for the query. Other query logics may have
 * other uses for postprocessing aside from reducing/combining results.
 */
public interface ResultPostprocessor {
    /**
     * The apply method is called each time a result is added to the list.
     *
     * @param results
     *            The results to be returned to the user
     */
    void apply(List<Object> results);

    /**
     * Used to get any cached summary results into a results queue
     *
     * @return an iterable of results
     */
    default Iterator<Object> flushResults() {
        return Collections.emptyIterator();
    }

    /**
     * Used to update a configuration with state required to be saved across pages of results.
     *
     * @param config
     */
    default void saveState(GenericQueryConfiguration config) {}

    class IdentityResultPostprocessor implements ResultPostprocessor {
        public void apply(List<Object> results) {
            // do nothing
        }
    }
}

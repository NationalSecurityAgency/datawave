package datawave.core.query.logic;

import java.util.List;

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

    class IdentityResultPostprocessor implements ResultPostprocessor {
        public void apply(List<Object> results) {
            // do nothing
        }
    }
}

package datawave.query.planner.async;

import java.util.concurrent.Callable;

import datawave.query.util.QueryStopwatch;

/**
 * Generic interface that allows stage names to be associated with various tasks. Extending classes may pass in a {@link QueryStopwatch} to capture timing
 * details of the operation.
 *
 * @param <T>
 *            the object type
 */
public abstract class AbstractQueryPlannerCallable<T> implements Callable<T> {

    protected QueryStopwatch timer;

    /**
     * Constructor that supports timing operations
     *
     * @param timer
     *            a stop watch
     */
    protected AbstractQueryPlannerCallable(QueryStopwatch timer) {
        this.timer = timer;
    }

    /**
     * The stage name used for one or more timers
     *
     * @return the stage name
     */
    public abstract String stageName();
}

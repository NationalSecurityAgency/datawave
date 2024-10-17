package datawave.query.planner.async;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

/**
 * Wrapper around Ivarator json serialization that allows for asynchronous execution.
 */
public class SerializeIvaratorCacheDirs extends AbstractQueryPlannerCallable<String> {

    private final DefaultQueryPlanner planner;
    private final ShardQueryConfiguration config;

    private TraceStopwatch stopwatch;

    public SerializeIvaratorCacheDirs(DefaultQueryPlanner planner, ShardQueryConfiguration config) {
        this(null, planner, config);
    }

    public SerializeIvaratorCacheDirs(QueryStopwatch timer, DefaultQueryPlanner planner, ShardQueryConfiguration config) {
        super(timer);
        this.planner = planner;
        this.config = config;
    }

    @Override
    public String call() throws Exception {
        if (timer != null) {
            stopwatch = timer.newStartedStopwatch(stageName());
        }

        String serialized = IvaratorCacheDirConfig.toJson(planner.getShuffledIvaratoCacheDirConfigs(config));

        if (stopwatch != null) {
            stopwatch.stop();
        }

        return serialized;
    }

    @Override
    public String stageName() {
        return "Serialize IvaratorCacheDirs";
    }
}

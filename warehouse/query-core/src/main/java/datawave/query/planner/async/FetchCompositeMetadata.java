package datawave.query.planner.async;

import java.util.Set;

import datawave.query.composite.CompositeMetadata;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

/**
 * A wrapper around {@link MetadataHelper#getCompositeMetadata(Set)} that allows for concurrent execution of expensive planner steps.
 */
public class FetchCompositeMetadata extends AbstractQueryPlannerCallable<CompositeMetadata> {

    private final MetadataHelper helper;
    private final Set<String> datatypes;

    private TraceStopwatch stopwatch;

    public FetchCompositeMetadata(MetadataHelper helper, Set<String> datatypes) {
        this(null, helper, datatypes);
    }

    public FetchCompositeMetadata(QueryStopwatch timer, MetadataHelper helper, Set<String> datatypes) {
        super(timer);
        this.helper = helper;
        this.datatypes = datatypes;
    }

    @Override
    public CompositeMetadata call() throws Exception {
        if (timer != null) {
            stopwatch = timer.newStartedStopwatch(stageName());
        }

        CompositeMetadata compositeMetadata = helper.getCompositeMetadata(datatypes);

        if (stopwatch != null) {
            stopwatch.stop();
        }

        return compositeMetadata;
    }

    @Override
    public String stageName() {
        return "Fetch CompositeMetadata";
    }
}

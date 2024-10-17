package datawave.query.planner.async;

import java.util.Set;

import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.query.util.TypeMetadata;
import datawave.util.time.TraceStopwatch;

/**
 * Wrapper around {@link MetadataHelper#getTypeMetadata(Set)} that allows for concurrent execution of expensive planner steps.
 */
public class FetchTypeMetadata extends AbstractQueryPlannerCallable<TypeMetadata> {

    private final MetadataHelper helper;
    private final Set<String> datatypes;

    private TraceStopwatch stopwatch;

    public FetchTypeMetadata(MetadataHelper helper, Set<String> datatypes) {
        this(null, helper, datatypes);
    }

    public FetchTypeMetadata(QueryStopwatch timer, MetadataHelper helper, Set<String> datatypes) {
        super(timer);
        this.helper = helper;
        this.datatypes = datatypes;
    }

    @Override
    public TypeMetadata call() throws Exception {
        if (timer != null) {
            stopwatch = timer.newStartedStopwatch(stageName());
        }

        TypeMetadata typeMetadata = helper.getTypeMetadata(datatypes);

        if (stopwatch != null) {
            stopwatch.stop();
        }

        return typeMetadata;
    }

    @Override
    public String stageName() {
        return "Fetch TypeMetadata";
    }
}

package datawave.query.planner.async;

import java.util.Set;

import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

public class FetchIndexedFields extends AbstractQueryPlannerCallable<Set<String>> {

    private final MetadataHelper helper;
    private final Set<String> datatypes;

    private TraceStopwatch stopwatch;

    public FetchIndexedFields(MetadataHelper helper, Set<String> datatypes) {
        this(null, helper, datatypes);
    }

    public FetchIndexedFields(QueryStopwatch timer, MetadataHelper helper, Set<String> datatypes) {
        super(timer);
        this.helper = helper;
        this.datatypes = datatypes;
    }

    @Override
    public Set<String> call() throws Exception {
        if (timer != null) {
            stopwatch = timer.newStartedStopwatch(stageName());
        }

        Set<String> indexedFields = helper.getIndexedFields(datatypes);

        if (stopwatch != null) {
            stopwatch.stop();
        }

        return indexedFields;
    }

    @Override
    public String stageName() {
        return "Fetch Indexed Fields";
    }

}

package datawave.query.planner.async;

import java.util.Set;

import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

public class FetchTermFrequencyFields extends AbstractQueryPlannerCallable<Set<String>> {

    private final MetadataHelper helper;
    private final Set<String> datatypes;

    private TraceStopwatch stopwatch;

    public FetchTermFrequencyFields(MetadataHelper helper, Set<String> datatypes) {
        this(null, helper, datatypes);
    }

    public FetchTermFrequencyFields(QueryStopwatch timer, MetadataHelper helper, Set<String> datatypes) {
        super(timer);
        this.helper = helper;
        this.datatypes = datatypes;
    }

    @Override
    public Set<String> call() throws Exception {
        if (timer != null) {
            stopwatch = timer.newStartedStopwatch(stageName());
        }

        Set<String> termFrequencyFields = helper.getTermFrequencyFields(datatypes);

        if (stopwatch != null) {
            stopwatch.stop();
        }

        return termFrequencyFields;
    }

    @Override
    public String stageName() {
        return "Fetch TermFrequency Fields";
    }
}

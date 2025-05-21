package datawave.query.planner.async;

import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

/**
 * A wrapper around the {@link MetadataHelper#getContentFields(Set)} method that allows for concurrent execution of expensive planner steps.
 */
public class FetchContentExpansionFields extends AbstractQueryPlannerCallable<String> {

    private static final Logger log = LoggerFactory.getLogger(FetchContentExpansionFields.class);

    private final MetadataHelper helper;
    private final Set<String> datatypes;

    private TraceStopwatch stopwatch;

    public FetchContentExpansionFields(MetadataHelper helper, Set<String> datatypes) {
        this(null, helper, datatypes);
    }

    public FetchContentExpansionFields(QueryStopwatch timer, MetadataHelper helper, Set<String> datatypes) {
        super(timer);
        this.helper = helper;
        this.datatypes = datatypes;
    }

    @Override
    public String call() {
        try {
            if (timer != null) {
                stopwatch = timer.newStartedStopwatch(stageName());
            }

            String fields = Joiner.on(',').join(helper.getContentFields(datatypes));

            if (stopwatch != null) {
                stopwatch.stop();
            }

            return fields;
        } catch (TableNotFoundException e) {
            log.error("Failed to fetch content expansion fields");
            throw new RuntimeException(e);
        }
    }

    @Override
    public String stageName() {
        return "Fetch ContentExpansionFields";
    }
}

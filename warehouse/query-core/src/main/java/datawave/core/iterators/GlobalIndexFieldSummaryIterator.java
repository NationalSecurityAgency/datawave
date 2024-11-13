package datawave.core.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * <p>
 * Iterator used for global index lookups in ShardIndexQueryTable. This iterator will aggregate information by date for a fieldValue, fieldName, and datatype.
 * This iterator is set up in the ShardIndexQueryTable and is constructed with a source iterator which is already filtering the requested datatypes
 * (GlobalIndexDataTypeFilter), term(s) (GlobalIndexTermMatchingFilter), and date range (GlobalIndexDateRangeFilter) as desired by the client.
 * </p>
 */
public class GlobalIndexFieldSummaryIterator extends GlobalIndexDateSummaryIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    protected static final Logger log = Logger.getLogger(GlobalIndexFieldSummaryIterator.class);

    public GlobalIndexFieldSummaryIterator() {
        super();
    }

    public GlobalIndexFieldSummaryIterator(GlobalIndexFieldSummaryIterator iter, IteratorEnvironment env) {
        super(iter, env);
    }

    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new GlobalIndexFieldSummaryIterator(this, env);
    }

    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        return new IteratorOptions(getClass().getSimpleName(), "returns global index keys aggregating fields names by type and date", options, null);
    }

    /**
     * This method aggregates all information from the global index by fieldname, day, and type
     */
    protected void findTop() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("findTop called");
        }

        // create a map of summaries by fieldValue, fieldName and date
        Map<String,TermInfoSummary> summaries = new HashMap<>();

        // Get the next valid term info
        TermInfo termInfo = getNextValidTermInfo();

        // while we have a term info
        while (termInfo != null) {
            String key = new StringBuilder().append(termInfo.fieldValue).append('\0').append(termInfo.fieldName).append('\0').append(termInfo.date).toString();
            if (!summaries.containsKey(key)) {
                summaries.put(key, new TermInfoSummary(termInfo.fieldValue, termInfo.fieldName, termInfo.date));
            }

            TermInfoSummary summary = summaries.get(key);
            summary.addTermInfo(termInfo);

            this.iterator.next();
            termInfo = getNextValidTermInfo();
        }

        for (TermInfoSummary summary : summaries.values()) {
            // now turn the summary into a set of key, value pairs
            returnCache.putAll(summary.getKeyValues());
        }

        if (log.isDebugEnabled()) {
            log.debug("findTop returning with " + returnCache.size() + " results");
        }
    }
}

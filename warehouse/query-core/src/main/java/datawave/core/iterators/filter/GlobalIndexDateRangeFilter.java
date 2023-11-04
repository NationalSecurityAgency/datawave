package datawave.core.iterators.filter;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import datawave.query.Constants;
import datawave.util.CompositeTimestamp;

/**
 * The iterator skips entries in the global index for entries that lie outside the date range set on the BatchScanner
 *
 */
public class GlobalIndexDateRangeFilter extends Filter {

    private LongRange range = null;
    private static final Logger log = Logger.getLogger(GlobalIndexDateRangeFilter.class);

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey(Constants.START_DATE) && options.containsKey(Constants.END_DATE)) {
            long start = Long.parseLong(options.get(Constants.START_DATE));
            long end = Long.parseLong(options.get(Constants.END_DATE));

            // The begin date from the query may be down to the second, for doing lookups in the index we want to use the day because
            // the times in the index table have been truncated to the day.
            start = DateUtils.truncate(new Date(start), Calendar.DAY_OF_MONTH).getTime();

            if (start > end)
                throw new IllegalArgumentException("Start date comes after end date");

            range = new LongRange(start, end);
            if (log.isDebugEnabled()) {
                log.debug("Set the date range to " + new Date(range.getMinimumLong()) + " to " + new Date(range.getMaximumLong()));
            }
        } else {
            throw new IllegalArgumentException("Both options must be set: " + Constants.START_DATE + " and " + Constants.END_DATE);
        }
    }

    public static void main(String[] args) {
        Date d = new Date();

        long t = d.getTime();
        System.out.println(t);

        System.out.println(DateUtils.truncate(new Date(t), Calendar.DATE).getTime());

        long days = Math.round((t / (24L * 3600L * 1000L)));
        System.out.println(days);
        t = days * (24L * 3600L * 1000L);
        System.out.println(t);

    }

    @Override
    public boolean accept(Key k, Value v) {
        return range.containsLong(CompositeTimestamp.getEventDate(k.getTimestamp()));

    }

}

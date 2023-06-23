package datawave.iterators.filter;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public abstract class AgeOffFilterBase extends Filter {
    private byte[] cutoffDate;
    private int cutoffDateLen;

    @Override
    public boolean accept(Key k, Value v) {
        // Keep the pair if its date is after the cutoff date

        byte[] dateBytes = getDateBytes(k, v);
        int dateLen = Math.min(dateBytes.length, cutoffDateLen);
        int result = WritableComparator.compareBytes(dateBytes, 0, dateLen, cutoffDate, 0, cutoffDateLen);
        return result > 0;
    }

    protected abstract byte[] getDateBytes(Key k, Value v);

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options == null)
            throw new IllegalArgumentException("ttl must be set for DateBasedAgeOffFilter");

        String ttl = options.get("ttl");
        if (ttl == null)
            throw new IllegalArgumentException("ttl must be set for DateBasedAgeOffFilter");

        int thresholdDays = Integer.parseInt(ttl);
        Text cutoffDateText = new Text(DateHelper.format(getCutoffDate(thresholdDays)));
        cutoffDate = cutoffDateText.getBytes();
        cutoffDateLen = cutoffDateText.getLength();
    }

    protected Date getCutoffDate(int thresholdDays) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.DAY_OF_YEAR, -(thresholdDays + 1)); // need to add one for comparison (keep something for one day means the date has to be after two
                                                             // days ago)
        return cal.getTime();
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption("ttl", "time to live (days)");
        io.setName("ageoff");
        io.setDescription("DateBasedAgeOffFilter removes entries with dates more than <ttl> days old");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);
        if (valid) {
            try {
                Integer.parseInt(options.get("ttl"));
            } catch (Exception e) {
                valid = false;
            }
        }
        return valid;
    }

}

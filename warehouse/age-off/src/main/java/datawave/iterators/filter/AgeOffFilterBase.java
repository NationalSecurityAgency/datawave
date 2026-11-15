package datawave.iterators.filter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import datawave.util.time.DateHelper;

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

    /**
     * Return the start of the cutoff day in the default time zone. One extra day is subtracted because entries must be strictly after the cutoff.
     *
     * @param thresholdDays
     *            the number of days to retain
     * @return the cutoff date
     */
    protected Date getCutoffDate(int thresholdDays) {
        ZoneId zone = ZoneId.systemDefault();
        LocalDate cutoffDay = LocalDate.now(zone).minusDays((long) thresholdDays + 1);
        return Date.from(cutoffDay.atStartOfDay(zone).toInstant());
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

package datawave.mapreduce.shardStats;

import org.apache.accumulo.core.data.Value;

import java.io.IOException;

/**
 * Summary for all values processed by the reducer for a single field name/datatype pair.
 */
public interface FieldSummary {

    /**
     * Returns the total count of all values for a field.
     *
     * @return total field count
     */
    long getCount();

    /**
     * Adds values to the current set of values.
     *
     * @param value
     *            field name/datatype pair summary values
     * @throws IOException
     *             invalid value
     */
    void add(Value value) throws IOException;

    /**
     * Converts the contents to a {@link StatsCounters} object.
     *
     * @return populated {@link StatsCounters}
     */
    StatsCounters toStatsCounters();
}

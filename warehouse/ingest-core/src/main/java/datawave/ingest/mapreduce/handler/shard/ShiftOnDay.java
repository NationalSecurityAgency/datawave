package datawave.ingest.mapreduce.handler.shard;

import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.RawRecordContainer;
import datawave.util.time.DateHelper;

/**
 * Implementation of {@link ShardIdGenerator} that will shift the partition of a baseline shard ID for a record by the max number of shards. Given some shard id
 * {@code 20240110_2}, and a max shards of 10, the shard id will become {@code 20240110_12}. A {@link ShiftOnDay} can be configured with a set of data types, a
 * beginning date (inclusive), and an end date (inclusive). These attributes will be used by {@link ShiftOnDay#isApplicable(RawRecordContainer)} to determine if
 * the default shard id for that particular record should be replaced by {@link ShiftOnDay#getShardId(RawRecordContainer, String, int)}.
 */
public class ShiftOnDay implements ShardIdGenerator {

    /**
     * The configuration property for the comma-delimited datatypes of a {@link ShiftOnDay}.
     */
    public static final String DATATYPES = "datatypes";

    /**
     * The configuration property for the begin date of a {@link ShiftOnDay}.
     */
    public static final String BEGIN = "begin";

    /**
     * The configuration property for the end date of a {@link ShiftOnDay}.
     */
    public static final String END = "end";

    private final Set<String> dataTypes;
    private final Date begin;
    private final Date end;

    public ShiftOnDay(Set<String> dataTypes, Date begin, Date end) {
        this.dataTypes = dataTypes == null ? Set.of() : Set.copyOf(dataTypes);
        this.begin = begin == null ? null : new Date(begin.getTime());
        this.end = end == null ? null : new Date(end.getTime());
        validateDateRange(begin, end);
    }

    public ShiftOnDay(Configuration conf, String property) {
        // Parse the data types.
        String dataTypesStr = conf.get((property + "." + DATATYPES));
        this.dataTypes = StringUtils.isBlank(dataTypesStr) ? Set.of() : Set.of(datawave.util.StringUtils.split(dataTypesStr, ','));

        // Parse the date range.
        String beginStr = conf.get((property + "." + BEGIN));
        begin = StringUtils.isBlank(beginStr) ? null : DateHelper.parse(beginStr);
        String endStr = conf.get((property + "." + END));
        end = StringUtils.isBlank(endStr) ? null : DateHelper.parse(endStr);

        // Validate the date range.
        validateDateRange(begin, end);
    }

    /**
     * Throws an {@link IllegalArgumentException} if both the begin and end are non-null, and the begin date is after the end date.
     *
     * @param begin
     *            the begin date
     * @param end
     *            the end date
     */
    private void validateDateRange(Date begin, Date end) {
        if (begin != null && end != null) {
            if (begin.getTime() > end.getTime()) {
                throw new IllegalArgumentException("End date must be after begin date");
            }
        }
    }

    /**
     * Returns whether this {@link ShiftOnDay} is considered applicable for the given record. It is considered applicable if:
     * <ol>
     * <li>This {@link ShiftOnDay} has no datatypes to filter on, or has a datatype that matches the data type of the record.</li>
     * <li>This {@link ShiftOnDay} has no begin date to filter on, or the record's date matches or occurs after the begin date.</li>
     * <li>This {@link ShiftOnDay} has no end date to filter on, or the record's date matches or occurs before the begin date.</li>
     * </ol>
     *
     * @param record
     *            the event record
     * @return true if this {@link ShiftOnDay} is considered applicable for the given record, or false otherwise.
     */
    @Override
    public boolean isApplicable(RawRecordContainer record) {
        // Check the data type.
        if (!dataTypes.isEmpty() && !dataTypes.contains(record.getDataType().typeName())) {
            return false;
        }

        if (begin != null || end != null) {
            // Check the record date.
            long recordDate = record.getDate();
            if (begin != null) {
                if (recordDate < begin.getTime()) {
                    return false;
                }
            }
            if (end != null) {
                return recordDate <= end.getTime();
            }

        }

        return true;
    }

    /**
     * Returns a modified form of the given sharded id where the partition portion of the shard id is shifted by the value of numShards. For example, given a
     * value of {@code "20240110_2"} for baseShardId, and a value of 10 for numShards, {@code "20240110_12"} will be returned.
     *
     * @param record
     *            the record
     * @param baseShardId
     *            the base shard id
     * @param numShards
     *            the number of shards
     * @return the modified shard id
     */
    @Override
    public String getShardId(RawRecordContainer record, String baseShardId, int numShards) {
        String[] parts = datawave.util.StringUtils.split(baseShardId, '_');
        // Shift the partition portion of the shard ID by the max number of shards.
        int partition = Integer.parseInt(parts[1]) + numShards;
        // Return the rebuilt shard id.
        return parts[0] + '_' + partition;
    }

    public Set<String> getDataTypes() {
        return Set.copyOf(dataTypes);
    }

    public Date getBegin() {
        return begin == null ? null : new Date(begin.getTime());
    }

    public Date getEnd() {
        return end == null ? null : new Date(end.getTime());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ShiftOnDay that = (ShiftOnDay) object;
        return Objects.equals(dataTypes, that.dataTypes) && Objects.equals(begin, that.begin) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataTypes, begin, end);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ShiftOnDay.class.getSimpleName() + "[", "]").add("dataTypes=" + dataTypes).add("begin=" + begin).add("end=" + end)
                        .toString();
    }
}

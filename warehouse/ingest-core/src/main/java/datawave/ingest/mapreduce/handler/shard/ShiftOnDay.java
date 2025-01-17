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
 * Implementation of {@link ShardIdGenerator} that will
 */
public class ShiftOnDay implements ShardIdGenerator {

    public static final String DATATYPES = "datatypes";
    public static final String BEGIN = "begin";
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

    private void validateDateRange(Date begin, Date end) {
        if (begin != null && end != null) {
            if (begin.getTime() > end.getTime()) {
                throw new IllegalArgumentException("End date must be after begin date");
            }
        }
    }

    @Override
    public boolean isApplicable(RawRecordContainer record) {
        if (!dataTypes.isEmpty() && !dataTypes.contains(record.getDataType().typeName())) {
            return false;
        }

        if (begin != null || end != null) {
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

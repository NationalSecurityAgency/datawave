package datawave.ingest.mapreduce.handler.shard;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.util.time.DateHelper;

/**
 * FieldDateShardIdGenerator ia an implementation of {@link ShardIdGenerator} that will shift the range of shard IDs based on an event date range and fields
 * within the record. For events that do not match one of the configured fields, they will be remapped to a base set of shards. It is expected that the
 * configured shards for that day will be set to the total number of shards possible for query purposes.
 *
 * So for example:
 *
 * @formatter:off
 * shardIdFactory.generator.1 = datawave.ingest.mapreduce.handler.shard.FieldDateShardIdGenerator
 * shardIdFactory.generator.1.baseNumShards = 20
 * shardIdFactory.generator.1.startDate = 20250101000000
 * shardIdFactory.generator.1.endDate = 20250103235959
 * shardIdFactory.generator.1.datatype.1 = DT1
 * shardIdFactory.generator.1.field.1 = FIELD1
 * shardIdFactory.generator.1.regex.1 = REGEX1
 * shardIdFactory.generator.1.shards.1 = 10
 * shardIdFactory.generator.1.datatype.2 = DT2
 * shardIdFactory.generator.1.field.2 = FIELD2
 * shardIdFactory.generator.1.regex.2 = REGEX2
 * shardIdFactory.generator.1.shards.2 = 15
 * @formatter:on
 *
 * In this case the shards for the day should be set to 45 and events that fall in the range of 20250101-20250103 will be processed as follows:
 *
 * an event with DT1 and FIELD1 matching REGEX1 will get mapped to shards 20-29. an event with DT2 and FIELD2 matching REGEX2 will get mapped
 * to shards 30-44 all other events will get mapped to shards 0-19
 *
 **/
public class FieldDateShardIdGenerator implements ShardIdGenerator {

    /**
     * The configuration property for the base number of shards of a {@link FieldDateShardIdGenerator}.
     */
    public static final String BASENUMSHARDS = "baseNumShards";

    /**
     * The configuration property for the begin date of a {@link FieldDateShardIdGenerator}.
     */
    public static final String BEGIN = "startDate";

    /**
     * The configuration property for the end date of a {@link FieldDateShardIdGenerator}.
     */
    public static final String END = "endDate";

    public static final String DATATYPE = "datatype";
    public static final String FIELD = "field";
    public static final String REGEX = "regex";
    public static final String SHARDS = "shards";

    private final Date begin;
    private final Date end;
    private final int baseNumShards;
    private final List<FieldMapping> mappings;

    static class FieldMapping {
        private final String datatype;
        private final String field;
        private final String regex;
        private final int shards;
        private Pattern pattern;

        public FieldMapping(String datatype, String field, String regex, int shards) {
            this.datatype = datatype;
            this.field = field;
            this.regex = regex;
            this.shards = shards;
        }

        public String getDatatype() {
            return datatype;
        }

        public String getField() {
            return field;
        }

        public String getRegex() {
            return regex;
        }

        public Pattern getPattern() {
            if (pattern == null) {
                pattern = Pattern.compile(regex);
            }
            return pattern;
        }

        public int getShards() {
            return shards;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FieldMapping))
                return false;
            FieldMapping that = (FieldMapping) o;
            return shards == that.shards && Objects.equals(datatype, that.datatype) && Objects.equals(field, that.field) && Objects.equals(regex, that.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datatype, field, regex, shards);
        }

        @Override
        public String toString() {
            return "FieldMapping{" + "datatype='" + datatype + '\'' + ", field='" + field + '\'' + ", regex='" + regex + '\'' + ", shards=" + shards + '}';
        }
    }

    public FieldDateShardIdGenerator(int baseNumShards, Date begin, Date end, List<FieldMapping> mappings) {
        this.baseNumShards = baseNumShards;
        validateBaseNumShards(baseNumShards);
        this.begin = begin == null ? null : new Date(begin.getTime());
        this.end = end == null ? null : new Date(end.getTime());
        validateDateRange(begin, end);
        this.mappings = mappings;
        validateMappings(mappings);
    }

    public FieldDateShardIdGenerator(Configuration conf, String property) {
        // base the base number of shards
        baseNumShards = conf.getInt(property + "." + BASENUMSHARDS, -1);
        validateBaseNumShards(baseNumShards);

        // Parse the date range.
        String beginStr = conf.get((property + "." + BEGIN));
        if (!StringUtils.isBlank(beginStr)) {
            if (beginStr.length() == 8) {
                beginStr = beginStr + "000000";
            }
            begin = DateHelper.parseTimeExactToSeconds(beginStr);
        } else {
            begin = null;
        }
        String endStr = conf.get((property + "." + END));
        if (!StringUtils.isBlank(endStr)) {
            if (endStr.length() == 8) {
                endStr = endStr + "235959";
            }
            end = DateHelper.parseTimeExactToSeconds(endStr);
        } else {
            end = null;
        }
        validateDateRange(begin, end);

        // Parse the field mappings
        int entry = 1;
        boolean foundEntry = false;
        mappings = new ArrayList<>();
        do {
            foundEntry = false;
            FieldMapping mapping = new FieldMapping(conf.get(property + "." + DATATYPE + "." + entry), conf.get(property + "." + FIELD + "." + entry),
                            conf.get(property + "." + REGEX + "." + entry), conf.getInt(property + "." + SHARDS + "." + entry, -1));
            if (mapping.getDatatype() != null || mapping.getField() != null || mapping.getRegex() != null || mapping.getShards() != -1) {
                mappings.add(mapping);
                entry++;
                foundEntry = true;
            }
        } while (foundEntry);
        validateMappings(mappings);
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
                throw new IllegalArgumentException("End date must be after begin date (" + begin + " <= " + end);
            }
        }
    }

    private void validateBaseNumShards(int baseNumShards) {
        if (baseNumShards < 0) {
            throw new IllegalArgumentException("Base num shards must be non-negative: " + baseNumShards);
        }
    }

    private void validateMappings(List<FieldMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("Must configure at least one field mapping");
        }
        for (FieldMapping mapping : mappings) {
            if (mapping.getDatatype() == null || mapping.getField() == null || mapping.getRegex() == null || mapping.getShards() < 0) {
                throw new IllegalArgumentException("Must configure all of datatype, field, value, and shards for a field mapping: " + mapping);
            }
            // getting the pattern will test compiling the regex
            try {
                mapping.getPattern();
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Illegal regex " + mapping.getRegex(), e);
            }
        }
    }

    /**
     * Returns whether this {@link FieldDateShardIdGenerator} is considered applicable for the given record. It is considered applicable if:
     * <ol>
     * <li>This {@link FieldDateShardIdGenerator} has no datatypes to filter on, or has a datatype that matches the data type of the record.</li>
     * <li>This {@link FieldDateShardIdGenerator} has no begin date to filter on, or the record's date matches or occurs after the begin date.</li>
     * <li>This {@link FieldDateShardIdGenerator} has no end date to filter on, or the record's date matches or occurs before the begin date.</li>
     * </ol>
     *
     * @param record
     *            the event record
     * @return true if this {@link FieldDateShardIdGenerator} is considered applicable for the given record, or false otherwise.
     */
    @Override
    public boolean isApplicable(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields) {
        boolean dateMatches = true;
        if (begin != null || end != null) {
            // Check the record date.
            long recordDate = record.getDate();
            if (begin != null) {
                if (recordDate < begin.getTime()) {
                    dateMatches = false;
                }
            }
            if (end != null) {
                if (recordDate > end.getTime()) {
                    dateMatches = false;
                }
            }
        }
        if (dateMatches) {
            for (FieldMapping mapping : this.mappings) {
                if (mayMatch(record, eventFields, mapping)) {
                    return true;
                }
            }
        }
        return false;
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
    public String getShardId(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields, String baseShardId, int numShards) {
        int shardStart = this.baseNumShards;
        for (FieldMapping mapping : this.mappings) {
            if (matches(record, eventFields, mapping)) {
                return shiftShardId(ShardIdFactory.getBaseShardId(record, mapping.shards), shardStart);
            } else {
                shardStart += mapping.shards;
            }
        }

        return ShardIdFactory.getBaseShardId(record, this.baseNumShards);
    }

    /**
     * Determine is a record matches a field mapping
     *
     * @param record
     *            The record
     * @param mapping
     *            The field mapping
     * @return true if matches
     */
    private boolean matches(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields, FieldMapping mapping) {
        return (record.getDataType().typeName().equals(mapping.datatype) || record.getDataType().outputName().equals(mapping.datatype))
                        && getFieldValues(record, eventFields, mapping.field).stream().anyMatch(v -> mapping.pattern.matcher(v).matches());
    }

    /**
     * Determine is a record may match a field mapping in that it is the correct datatype
     *
     * @param record
     *            The record
     * @param mapping
     *            The field mapping
     * @return true if we may match
     */
    private boolean mayMatch(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields, FieldMapping mapping) {
        return (record.getDataType().typeName().equals(mapping.datatype) || record.getDataType().outputName().equals(mapping.datatype));
    }

    /**
     * Get the values for a field from a record
     *
     * @param record
     *            The record
     * @param field
     *            The field
     * @return the set of values
     */
    private Set<String> getFieldValues(RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields, String field) {
        Set<String> values = new HashSet<>();
        // first check the event fields
        if (eventFields != null && eventFields.containsKey(field)) {
            for (NormalizedContentInterface value : eventFields.get(field)) {
                values.add(value.getIndexedFieldValue());
                values.add(value.getEventFieldValue());
            }
        } else {
            // try pulling the field from the record
            try {
                Object value = getValue(record, field);
                if (value != null) {
                    values.add(value.toString());
                }
            } catch (Exception e) {
                // ignore
            }
        }
        return values;
    }

    public Object getValue(Object source, String fieldName) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        String getter = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return source.getClass().getMethod(getter).invoke(source);
        } catch (NoSuchMethodException e) {
            getter = "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            return source.getClass().getMethod(getter).invoke(source);
        }
    }

    /**
     * Shift a shard id by the specified number of shards
     *
     * @param baseShardId
     *            The base shard id
     * @param numShards
     *            The number of shards to shift by
     * @return the new shard id
     */
    private String shiftShardId(String baseShardId, int numShards) {
        String[] parts = datawave.util.StringUtils.split(baseShardId, '_');
        // Shift the partition portion of the shard ID by the max number of shards.
        int partition = Integer.parseInt(parts[1]) + numShards;
        // Return the rebuilt shard id.
        return parts[0] + '_' + partition;
    }

    public int getBaseNumShards() {
        return baseNumShards;
    }

    public Date getBegin() {
        return begin == null ? null : new Date(begin.getTime());
    }

    public Date getEnd() {
        return end == null ? null : new Date(end.getTime());
    }

    public List<FieldMapping> getMappings() {
        return mappings;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        FieldDateShardIdGenerator that = (FieldDateShardIdGenerator) object;
        return Objects.equals(baseNumShards, that.baseNumShards) && Objects.equals(begin, that.begin) && Objects.equals(end, that.end)
                        && Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseNumShards, begin, end, mappings);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FieldDateShardIdGenerator.class.getSimpleName() + "[", "]").add("baseNumShards=" + baseNumShards).add("begin=" + begin)
                        .add("end=" + end).add("mappings=" + mappings).toString();
    }
}

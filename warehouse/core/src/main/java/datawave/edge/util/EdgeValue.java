package datawave.edge.util;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.edge.protobuf.EdgeData;
import datawave.edge.protobuf.EdgeData.EdgeValue.Builder;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.UUID;

/**
 * Utility class for serializing edge table protocol buffer. Previously, the EdgeValueHelper class was sufficient for handling the edge values, but with the
 * addition of fields beyond the counts, a more generic solution is required.
 *
 */
public class EdgeValue {
    private final Long count;
    private final Integer bitmask;
    private final String sourceValue;
    private final String sinkValue;
    private final List<Long> hours;
    private final List<Long> duration;
    private String loadDate;
    private UUID uuidObj;
    // see EdgeData.proto. hasOnlyUuidString is true if uuid_string is/should be set in the protobuf message, meaning that uuid is not set because a UUID object
    // couldn't be created from the original uuid
    private boolean hasOnlyUuidString;
    // see hasOnlyUuidString. This is either the uuid_string value or if uuid_string is not set it contains a human readable version of uuidObj
    private String uuidString;
    private Boolean badActivityDate;

    private EdgeValue(Long count, Integer bitmask, String sourceValue, String sinkValue) {
        this(count, bitmask, sourceValue, sinkValue, null, null, null, null, false, null, null);
    }

    private EdgeValue(Long count, Integer bitmask, String sourceValue, String sinkValue, List<Long> hours, List<Long> duration, String loadDate,
                    String uuidString, boolean hasOnlyUuidString, UUID uuidObj, Boolean badActivityDate) {

        if (count == 0) {
            this.count = null;
        } else {
            this.count = count;
        }
        if (bitmask == 0) {
            this.bitmask = null;
        } else {
            this.bitmask = bitmask;
        }
        this.sourceValue = sourceValue;
        this.sinkValue = sinkValue;
        this.hours = hours;
        this.duration = duration;
        this.loadDate = loadDate;
        this.uuidString = uuidString;
        this.hasOnlyUuidString = hasOnlyUuidString;
        this.uuidObj = uuidObj;
        this.badActivityDate = badActivityDate;
    }

    private static final int[] HOUR_MASKS = {0x000001, 0x000002, 0x000004, 0x000008, 0x000010, 0x000020, 0x000040, 0x000080, 0x000100, 0x000200, 0x000400,
            0x000800, 0x001000, 0x002000, 0x004000, 0x008000, 0x010000, 0x020000, 0x040000, 0x080000, 0x100000, 0x200000, 0x400000, 0x800000};

    public static class EdgeValueBuilder {

        private Long count;
        private Integer bitmask;
        private String sourceValue = null;
        private String sinkValue = null;
        private List<Long> hours = null;
        private List<Long> duration = null;
        private String loadDate = null;
        private UUID uuidObj = null;
        private boolean hasOnlyUuidString = false;
        private String uuidString = null;
        private Boolean badActivityDate = null;

        private EdgeValueBuilder() {
            this.count = 0l;
            this.bitmask = 0;
        }

        private EdgeValueBuilder(EdgeValue edgeValue) {
            this.count = edgeValue.getCount();
            this.bitmask = edgeValue.getBitmask();
            this.sourceValue = edgeValue.getSourceValue();
            this.sinkValue = edgeValue.getSinkValue();
            this.hours = edgeValue.getHours();
            this.duration = edgeValue.getDuration();
            this.loadDate = edgeValue.getLoadDate();
            this.hasOnlyUuidString = edgeValue.hasOnlyUuidString;
            this.uuidObj = edgeValue.getUuidObject();
            this.uuidString = edgeValue.getUuid();
        }

        public EdgeValue build() {
            return new EdgeValue(this.count, this.bitmask, this.sourceValue, this.sinkValue, this.hours, this.duration, this.loadDate, this.uuidString,
                            this.hasOnlyUuidString, this.uuidObj, this.badActivityDate);

        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Integer getBitmask() {
            return bitmask;
        }

        public void setBitmask(Integer bitmask) {
            this.bitmask = bitmask;
        }

        public void setHour(Integer hour) {
            if (hour > 23 || hour < 0) {
                throw new IllegalArgumentException("Supplied Integer for hour bitmask is out of range: " + hour);
            }
            if (bitmask == null) {
                bitmask = 0;
            }
            bitmask |= HOUR_MASKS[hour];
        }

        public void combineBitmask(int otherMask) {
            this.bitmask |= otherMask;
        }

        public String getSourceValue() {
            return sourceValue;
        }

        public String getSinkValue() {
            return sinkValue;
        }

        public void setSourceValue(String sourceValue) {
            this.sourceValue = sourceValue;
        }

        public void setSinkValue(String sinkValue) {
            this.sinkValue = sinkValue;
        }

        public List<Long> getHours() {
            return hours;
        }

        public void setHours(List<Long> hours) {
            this.hours = hours;
        }

        public List<Long> getDuration() {
            return duration;
        }

        public void setDuration(List<Long> duration) {
            this.duration = duration;
        }

        public String getLoadDate() {
            return loadDate;
        }

        public void setLoadDate(String loadDate) {
            this.loadDate = loadDate;
        }

        public UUID getUuidObj() {
            if (this.hasOnlyUuidString) {
                return null;
            }
            if (null == uuidObj && StringUtils.isNotBlank(this.uuidString)) {
                try { // try to parse the uuid string to a UUID object
                    this.uuidObj = convertUuidStringToUuidObj(this.uuidString);
                } catch (Exception e) {
                    // if it failed to parse, settle for the uuid_string
                    this.hasOnlyUuidString = true;
                }
            }
            return uuidObj;
        }

        public void setUuidObj(UUID uuidObj) {
            this.uuidObj = uuidObj;
        }

        public String getUuid() {
            // if missing, attempt to initialize uuid from uuidObj
            if (StringUtils.isBlank(this.uuidString) && null != this.uuidObj) {
                this.uuidString = convertUuidObjectToString(this.uuidObj);
            }
            return this.uuidString;
        }

        public void setUuid(String uuid) {
            this.uuidString = uuid;
        }

        public Boolean isBadActivityDate() {
            return badActivityDate;
        }

        public void setBadActivityDate(Boolean badActivityDate) {
            this.badActivityDate = badActivityDate;
        }

        public boolean badActivityDateSet() {
            if (this.badActivityDate == null) {
                return false;
            } else {
                return true;
            }
        }

        public void setOnlyUuidString(boolean hasOnlyUuidString) {
            this.hasOnlyUuidString = hasOnlyUuidString;
        }
    }

    // ////// END BUILDER ////////

    public static EdgeValueBuilder newBuilder() {
        return new EdgeValueBuilder();
    }

    public static EdgeValueBuilder newBuilder(EdgeValue edgeValue) {
        return new EdgeValueBuilder(edgeValue);
    }

    public static EdgeValue decode(Value value) throws InvalidProtocolBufferException {

        EdgeData.EdgeValue proto = EdgeData.EdgeValue.parseFrom(value.get());

        EdgeValueBuilder builder = new EdgeValueBuilder();
        if (proto.hasCount()) {
            builder.setCount(proto.getCount());
        } else {
            builder.setCount(0l);
        }
        if (proto.hasHourBitmask()) {
            builder.setBitmask(proto.getHourBitmask());
        }
        if (proto.hasSourceValue()) {
            builder.setSourceValue(proto.getSourceValue());
        }
        if (proto.hasSinkValue()) {
            builder.setSinkValue(proto.getSinkValue());
        }
        List<Long> hoursList = proto.getHoursList();
        if (hoursList != null && hoursList.isEmpty() == false) {
            builder.setHours(hoursList);
        }
        List<Long> durationList = proto.getDurationList();
        if (durationList != null && durationList.isEmpty() == false) {
            builder.setDuration(durationList);
        }
        if (proto.hasLoadDate()) {
            builder.setLoadDate(proto.getLoadDate());
        }
        if (proto.hasUuid()) {
            builder.setOnlyUuidString(false);
            builder.setUuidObj(convertUuidObject(proto.getUuid()));
        } else if (proto.hasUuidString()) {
            // if there is a uuid string in the protobuf data, it means that we shouldn't have a uuid object at all
            builder.setOnlyUuidString(true);
            builder.setUuid(proto.getUuidString());
        }
        if (proto.hasBadActivity()) {
            builder.setBadActivityDate(proto.getBadActivity());
        }
        return builder.build();
    }

    public Value encode() {
        Builder builder = EdgeData.EdgeValue.newBuilder();
        if (this.hasCount()) {
            builder.setCount(this.getCount());
        } else {
            builder.setCount(0l);
        }
        if (this.hasBitmask()) {
            builder.setHourBitmask(this.getBitmask());
        }
        if (this.sourceValue != null) {
            builder.setSourceValue(this.sourceValue);
        }
        if (this.sinkValue != null) {
            builder.setSinkValue(this.sinkValue);
        }
        if (this.hours != null) {
            builder.addAllHours(this.hours);
        }
        if (this.duration != null) {
            builder.addAllDuration(this.duration);
        }
        if (this.loadDate != null) {
            builder.setLoadDate(loadDate);
        }
        // iff the string uuid couldn't parse into a uuid object, the protobuf edge will contain uuid_string and not the uuid object
        if (this.hasOnlyUuidString) { // we know in advance that this has only the uuid_string (parsing must have already failed)
            if (StringUtils.isNotBlank(this.uuidString)) { // as long as the uuid isn't empty, set it
                builder.setUuidString(this.uuidString);
            }
        } else if (this.uuidObj != null) { // already have the uuid object, so there's no reason to reparse the string
            builder.setUuid(convertUuidObject(this.uuidObj));
        } else if (StringUtils.isNotBlank(this.uuidString)) {
            try { // try to parse the uuid string to a UUID object
                this.uuidObj = convertUuidStringToUuidObj(this.uuidString);
                builder.setUuid(convertUuidObject(this.uuidObj));
            } catch (Exception e) {
                // if it failed to parse, settle for the uuid_string
                this.hasOnlyUuidString = true;
                builder.setUuidString(this.uuidString);
            }
        }

        if (this.badActivityDate != null) {
            builder.setBadActivity(badActivityDate);
        }

        return new Value(builder.build().toByteArray());
    }

    public static UUID convertUuidStringToUuidObj(String uuidString) {
        return UUID.fromString(uuidString);
    }

    public Long getCount() {
        return count;
    }

    public boolean hasCount() {
        return this.count != null;
    }

    public Integer getBitmask() {
        if (null == bitmask)
            return 0;
        return bitmask;
    }

    public boolean hasBitmask() {
        return this.bitmask != null;
    }

    public boolean hasLoadDate() {
        return this.loadDate != null;
    }

    public boolean isHourSet(int hour) {
        if (hour > 23 || hour < 0) {
            throw new IllegalArgumentException("Supplied Integer for hour bitmask lookup is out of range: " + hour);
        }
        if (!this.hasBitmask()) {
            throw new IllegalStateException("No bitmask is defined.");
        }
        return ((this.bitmask & HOUR_MASKS[hour]) > 0);
    }

    public String getSourceValue() {
        return sourceValue;
    }

    public String getSinkValue() {
        return sinkValue;
    }

    public List<Long> getHours() {
        return hours;
    }

    public List<Long> getDuration() {
        return duration;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public UUID getUuidObject() {
        if (this.hasOnlyUuidString) {
            return null;
        }
        if (null == uuidObj && StringUtils.isNotBlank(this.uuidString)) {
            try { // try to parse the uuid string to a UUID object
                this.uuidObj = convertUuidStringToUuidObj(this.uuidString);
            } catch (Exception e) {
                // if it failed to parse, settle for the uuid_string
                this.hasOnlyUuidString = true;
            }
        }
        return uuidObj;
    }

    public String getUuid() {
        // if human readable string is missing, attempt to create one from the uuidObj
        if ((null == this.uuidString || this.uuidString.isEmpty()) && null != this.uuidObj) {
            this.uuidString = convertUuidObjectToString(this.uuidObj);
        }
        return this.uuidString;
    }

    public static String convertUuidObjectToString(UUID rawUuid) {
        return rawUuid.toString();
    }

    public static UUID convertUuidObject(EdgeData.EdgeValue.UUID rawUuid) {
        return new UUID(rawUuid.getMostSignificantBits(), rawUuid.getLeastSignificantBits());
    }

    public static EdgeData.EdgeValue.UUID convertUuidObject(UUID rawUuid) {
        EdgeData.EdgeValue.UUID.Builder builder = EdgeData.EdgeValue.UUID.newBuilder();
        builder.setLeastSignificantBits(rawUuid.getLeastSignificantBits());
        builder.setMostSignificantBits(rawUuid.getMostSignificantBits());
        return builder.build();
    }

    public Boolean isBadActivityDate() {
        return badActivityDate;
    }

    public void setBadActivityDate(Boolean badActivityDate) {
        this.badActivityDate = badActivityDate;
    }

    public boolean badActivityDateSet() {
        if (this.badActivityDate == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        EdgeValue edgeValue = (EdgeValue) o;

        if (count != null ? !count.equals(edgeValue.count) : edgeValue.count != null)
            return false;
        if (bitmask != null ? !bitmask.equals(edgeValue.bitmask) : edgeValue.bitmask != null)
            return false;
        if (sourceValue != null ? !sourceValue.equals(edgeValue.sourceValue) : edgeValue.sourceValue != null)
            return false;
        if (sinkValue != null ? !sinkValue.equals(edgeValue.sinkValue) : edgeValue.sinkValue != null)
            return false;
        if (hours != null ? !hours.equals(edgeValue.hours) : edgeValue.hours != null)
            return false;
        if (duration != null ? !duration.equals(edgeValue.duration) : edgeValue.duration != null)
            return false;
        if (loadDate != null ? !loadDate.equals(edgeValue.loadDate) : edgeValue.loadDate != null)
            return false;
        if (badActivityDate != null ? !badActivityDate.equals(edgeValue.badActivityDate) : edgeValue.badActivityDate != null)
            return false;

        // a protobuf Edge will have either a UUID object, preferrably, or a uuidString - but not both
        // the uuidObj and uuidString are both lazily initialized
        if (StringUtils.isNotBlank(uuidString) && StringUtils.isNotBlank(edgeValue.uuidString)) {
            return uuidString.equals(edgeValue.uuidString);
        } else if (null != uuidObj && null != edgeValue.uuidObj) {
            return uuidObj.equals(edgeValue.uuidObj);
        } else if (null != uuidObj && edgeValue.hasOnlyUuidString) {
            return false;
        } else if (null != edgeValue.uuidObj && edgeValue.hasOnlyUuidString) {
            return false;
        } else { // getUuid will force uuidString to get initialized if it isn't already
            return (getUuid() == null ? edgeValue.getUuid() == null : getUuid().equals(edgeValue.getUuid()));
        }
    }

    @Override
    public int hashCode() {
        int result = count != null ? count.hashCode() : 0;
        result = 31 * result + (bitmask != null ? bitmask.hashCode() : 0);
        result = 31 * result + (sourceValue != null ? sourceValue.hashCode() : 0);
        result = 31 * result + (sinkValue != null ? sinkValue.hashCode() : 0);
        result = 31 * result + (hours != null ? hours.hashCode() : 0);
        result = 31 * result + (duration != null ? duration.hashCode() : 0);
        result = 31 * result + (loadDate != null ? loadDate.hashCode() : 0);

        if (uuidObj == null && !hasOnlyUuidString) {
            // force initialization of uuidObj, if possible (will short circuit if already failed to parse)
            getUuidObject();
        }
        if (uuidObj != null) { // if it's impossible to use uuidObj, then use uuidString
            result = 31 * result + uuidObj.hashCode();
        } else {
            result = 31 * result + (uuidString != null ? uuidString.hashCode() : 0);
        }

        return 31 * result + (badActivityDate != null ? badActivityDate.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "count: " + (count != null ? count.toString() : "") + ", bitmask: " + (bitmask != null ? bitmask.toString() : "") + ", sourceValue: "
                        + (sourceValue != null ? sourceValue.toString() : "") + ", sinkValue: " + (sinkValue != null ? sinkValue.toString() : "") + ", hours: "
                        + (hours != null ? hours.toString() : "") + ", duration: " + (duration != null ? duration.toString() : "") + ", loadDate: "
                        + (loadDate != null ? loadDate.toString() : "") + ", uuidString: "
                        + (hasOnlyUuidString && uuidString != null ? uuidString.toString() : "") + ", uuidObj: " + (uuidObj != null ? uuidObj.toString() : "")
                        + ", badActivityDate: " + (badActivityDate != null ? badActivityDate.toString() : "");
    }
}

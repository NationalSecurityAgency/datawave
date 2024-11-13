package datawave.query.cardinality;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

public class DateFieldValueCardinalityRecord implements Comparable, Serializable {
    private static final long serialVersionUID = 1L;
    private String eventDate = null;
    private String fieldName = null;
    private String fieldValue = null;
    private String dataType = null;
    private HyperLogLogPlus hll = new HyperLogLogPlus(12, 20);
    private static Logger log = Logger.getLogger(DateFieldValueCardinalityRecord.class);

    public DateFieldValueCardinalityRecord(String eventDate, String fieldName, String fieldValue, String dataType) {
        this.eventDate = eventDate;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.dataType = dataType;
    }

    public DateFieldValueCardinalityRecord(DateFieldValueCardinalityRecord other) {
        this.eventDate = other.eventDate;
        this.fieldName = other.fieldName;
        this.fieldValue = other.fieldValue;
        this.dataType = other.dataType;
        try {
            this.hll.addAll(other.hll);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public long getCardinalityValue() {
        return hll.cardinality();
    }

    public HyperLogLogPlus getCardinality() {
        return hll;
    }

    public void setCardinality(HyperLogLogPlus cardinality) {
        this.hll = cardinality;
    }

    public void setCardinalityBytes(byte[] bytes) throws IOException {
        this.hll = HyperLogLogPlus.Builder.build(bytes);
    }

    public long getSize() {
        long size = 0;
        try {
            size = eventDate.getBytes().length + fieldName.getBytes().length + fieldValue.getBytes().length + hll.getBytes().length;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return size;
    }

    public byte[] getCardinalityBytes() {
        byte[] bytes = null;
        try {
            bytes = hll.getBytes();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return bytes;
    }

    public void addEventId(String eventId) {
        hll.offer(eventId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DateFieldValueCardinalityRecord that = (DateFieldValueCardinalityRecord) o;

        if (!eventDate.equals(that.eventDate)) {
            return false;
        }
        if (!dataType.equals(that.dataType)) {
            return false;
        }
        if (!fieldName.equals(that.fieldName)) {
            return false;
        }
        if (!fieldValue.equals(that.fieldValue)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash(this.eventDate, this.fieldName, this.fieldValue, this.dataType);
    }

    public static int hash(String eventDate, String fieldName, String fieldValue, String dataType) {
        int result = fieldName.hashCode();
        result = 31 * result + fieldValue.hashCode();
        result = 31 * result + eventDate.hashCode();
        result = 31 * result + dataType.hashCode();
        return result;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof DateFieldValueCardinalityRecord) {
            DateFieldValueCardinalityRecord other = (DateFieldValueCardinalityRecord) o;
            CompareToBuilder builder = new CompareToBuilder();
            builder.append(this.eventDate, other.eventDate);
            builder.append(this.dataType, other.dataType);
            builder.append(this.fieldName, other.fieldName);
            builder.append(this.fieldValue, other.fieldValue);
            return builder.toComparison();
        } else {
            throw new UnsupportedOperationException("Object " + o + " is not instanceof " + DateFieldValueCardinalityRecord.class.getCanonicalName());
        }
    }

    @Override
    public String toString() {
        return "FieldValueCardinality{" + "eventDate='" + eventDate + "'" + ", fieldName='" + fieldName + "'" + ", fieldValue='" + fieldValue + "'"
                        + ", dataType='" + dataType + "'" + ", hll=" + hll.cardinality() + '}';
    }

    public void merge(DateFieldValueCardinalityRecord other) {

        if (this.eventDate.equals(other.eventDate) == false) {
            throw new IllegalArgumentException("DateFieldValueCardinalityRecords have different eventDates: " + this.eventDate + " != " + other.eventDate);
        }
        if (this.fieldName.equals(other.fieldName) == false) {
            throw new IllegalArgumentException("DateFieldValueCardinalityRecords have different fieldNames: " + this.fieldName + " != " + other.fieldName);
        }
        if (this.fieldValue.equals(other.fieldValue) == false) {
            throw new IllegalArgumentException("DateFieldValueCardinalityRecords have different fieldValues: " + this.fieldValue + " != " + other.fieldValue);
        }
        if (this.dataType.equals(other.dataType) == false) {
            throw new IllegalArgumentException("DateFieldValueCardinalityRecords have different dataType: " + this.dataType + " != " + other.dataType);
        }
        try {
            this.hll.addAll(other.hll);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}

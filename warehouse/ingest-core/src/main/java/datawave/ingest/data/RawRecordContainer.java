package datawave.ingest.data;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.data.hash.UID;
import datawave.util.CompositeTimestamp;

/**
 * Generic container used to hold raw source data. It is used in various parts of the ingest framework and is typically persisted as an "event" within DW's
 * shard table schema
 */
public interface RawRecordContainer {

    Map<String,String> getSecurityMarkings();

    void setSecurityMarkings(Map<String,String> securityMarkings);

    void addSecurityMarking(String domain, String marking);

    boolean hasSecurityMarking(String domain, String marking);

    UID getId();

    void setId(UID id);

    /**
     * This method will generate the uid given the current state of the event
     *
     * @param uidExtra
     *            If not null, this is added into the uid
     */
    void generateId(String uidExtra);

    Type getDataType();

    void setDataType(Type dataType);

    /**
     * Gets the primary date associated with the record, a.k.a the "event date"
     *
     * @return the date for this raw record
     */
    default long getDate() {
        return CompositeTimestamp.getEventDate(getTimestamp());
    }

    /**
     * Gets the ageoff date associated with the record
     *
     * @return the ageoff date for this raw record
     */
    default long getAgeOffDate() {
        return CompositeTimestamp.getAgeOffDate(getTimestamp());
    }

    /**
     * Get the composite timestamp asociated with this record (@see CompositeTimestamp)
     *
     * @return
     */
    long getTimestamp();

    /**
     * Determine if the timestamp has been set. This avoids having to compare the timestamp with an arbitrary value
     */
    default boolean isTimestampSet() {
        return getTimestamp() != CompositeTimestamp.INVALID_TIMESTAMP;
    }

    /**
     * This is synonomis with setTimestamp(date, date)
     *
     * @param timestamp
     *            primary date to be associated with the record, a.k.a. the "event date"
     */
    @Deprecated
    default void setDate(long timestamp) {
        setTimestamp(timestamp);
    }

    /**
     * In the DW data model, this date is often referred to as "event date" and represents the primary date value for the record. At ingest time, it is
     * typically encoded as part of the Accumulo row id (in YYYYMMDD format) for the record's associated shard table entries. Clients may attach any semantics
     * they wish to this date, but should be aware that it directly impacts the sort order of their data in Accumulo.
     *
     * <p>
     * Thus, this date is typically leveraged by DW's query api as the basis for 'begin' / 'end' date ranges for user queries. However, DW also has the ability
     * to leverage other dates within your records at query time, if needed. So, for date filtering concerns, you're not necessarily stuck with your choice of
     * 'event' date in this regard
     * </p>
     *
     * <p>
     * This date is treated as a composite timestamp which includes the age off date as well. If the ageoff date is identical to the event date (which is
     * usually the case), then the event date and the timestamp will be the exactly same value. See CompositeTimestamp for more information. The getDate()
     * method will only return the event date portion of this date and the getAgeOffDate() will return the ageoff portion. This method should eventually be
     * deprecated and setTimestamp should be used instead.
     * </p>
     *
     * @param timestamp
     *            primary date to be associated with the record, a.k.a. the "event date"
     */
    void setTimestamp(long timestamp);

    /**
     * Equivalent to setTimestamp(CompositeTimestamp.getCompositeTimestamp(eventDate, ageOffDate));
     *
     * @param eventDate
     * @param ageOffDate
     */
    default void setTimestamp(long eventDate, long ageOffDate) {
        setTimestamp(CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOffDate));
    }

    Collection<String> getErrors();

    void setErrors(Collection<String> errors);

    void addError(String error);

    void removeError(String error);

    boolean hasError(String error);

    boolean fatalError();

    boolean ignorableError();

    void clearErrors();

    Collection<String> getAltIds();

    void setAltIds(Collection<String> altIds);

    void addAltId(String altId);

    boolean hasAltId(String altId);

    String getRawFileName();

    void setRawFileName(String rawFileName);

    long getRawRecordNumber();

    void setRawRecordNumber(long rawRecordNumber);

    long getRawFileTimestamp();

    void setRawFileTimestamp(long rawRecordTimestamp);

    byte[] getRawData();

    void setRawData(byte[] rawData);

    // Aux data is not intended to survive serialization
    Object getAuxData();

    void setAuxData(Object auxData);

    // Aux properties are not intended to survive serialization
    String getAuxProperty(String prop);

    void setAuxProperty(String prop, String value);

    RawRecordContainer copy();

    long getDataOutputSize();

    void write(DataOutput dataOutput) throws IOException;

    ColumnVisibility getVisibility();

    void setVisibility(ColumnVisibility visibility);

    /**
     * Return a date to use in the UID for this RawRecord
     *
     * @return null or date object
     */
    Date getTimeForUID();

    /**
     * Does this record need to be masked.
     *
     * @return true or false
     */
    boolean isRequiresMasking();

    /**
     * Clear/reset the current state to support object reuse
     */
    void clear();
}

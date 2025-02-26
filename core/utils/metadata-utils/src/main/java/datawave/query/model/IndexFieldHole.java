package datawave.query.model;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSortedSet;

/**
 * This class represents a set of calculated field index holes for a given fieldName and datatype. A field index hole is effectively a date where a frequency
 * row was seen, but an index and/or reversed indexed row was not.
 */
public class IndexFieldHole {
    
    private final String fieldName;
    private final String datatype;
    private final SortedSet<Pair<Date,Date>> dateRanges;
    private static long DAY_MILLIS = 1000L * 60 * 60 * 24;
    
    public IndexFieldHole(String fieldName, String dataType, Collection<Pair<Date,Date>> holes) {
        this.fieldName = fieldName;
        this.datatype = dataType;
        // Ensure the date range set is immutable, and starts at the beginning of the start date, and ends at the end of the end date
        ImmutableSortedSet.Builder<Pair<Date,Date>> builder = new ImmutableSortedSet.Builder<>(Comparator.naturalOrder());
        holes.forEach(p -> builder.add(new ImmutablePair<>(floor(p.getLeft()), ceil(p.getRight()))));
        dateRanges = builder.build();
    }
    
    /**
     * return the date instant at 00:00:00
     * 
     * @param d
     * @return instant of d with time reset to 00:00:00
     */
    private static Instant floorInstant(Date d) {
        return Instant.ofEpochMilli(d.getTime()).truncatedTo(ChronoUnit.DAYS);
    }
    
    /**
     * return the date at 00:00:00
     * 
     * @param d
     * @return d with time reset to 00:00:00
     */
    private static Date floor(Date d) {
        return Date.from(floorInstant(d));
    }
    
    /**
     * return the date at 23:59:59
     * 
     * @param d
     * @return d with time reset to 23:59:59
     */
    private static Date ceil(Date d) {
        return Date.from(floorInstant(d).plusMillis(DAY_MILLIS - 1));
    }
    
    /**
     * Return the field name.
     * 
     * @return the field name.
     */
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * Return the datatype.
     * 
     * @return the datatype.
     */
    public String getDatatype() {
        return datatype;
    }
    
    /**
     * Returns the set of date ranges that span over field index holes for the fieldName and datatype of this {@link IndexFieldHole}. Each date range represents
     * a span of consecutive days for which a frequency row exist, but an index row does not. All date ranges are start(inclusive)-end(inclusive).
     * 
     * @return the date ranges
     */
    public SortedSet<Pair<Date,Date>> getDateRanges() {
        return dateRanges;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFieldHole indexHole = (IndexFieldHole) o;
        return Objects.equals(fieldName, indexHole.fieldName) && Objects.equals(datatype, indexHole.datatype)
                        && Objects.equals(dateRanges, indexHole.dateRanges);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldName, datatype, dateRanges);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", IndexFieldHole.class.getSimpleName() + "[", "]").add("fieldName='" + fieldName + "'").add("dataType='" + datatype + "'")
                        .add("dateRanges=" + dateRanges).toString();
    }
}

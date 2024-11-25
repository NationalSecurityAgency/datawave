package datawave.query.model;

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
public class FieldIndexHole {
    
    private final String fieldName;
    private final String datatype;
    private final SortedSet<Pair<Date,Date>> dateRanges;
    
    public FieldIndexHole(String fieldName, String dataType, Collection<Pair<Date,Date>> holes) {
        this.fieldName = fieldName;
        this.datatype = dataType;
        // Ensure the date range set is immutable.
        ImmutableSortedSet.Builder<Pair<Date,Date>> builder = new ImmutableSortedSet.Builder<>(Comparator.naturalOrder());
        holes.forEach(p -> builder.add(new ImmutablePair<>(p.getLeft(), p.getRight())));
        dateRanges = builder.build();
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
     * Returns the set of date ranges that span over field index holes for the fieldName and datatype of this {@link FieldIndexHole}. Each date range represents
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
        FieldIndexHole indexHole = (FieldIndexHole) o;
        return Objects.equals(fieldName, indexHole.fieldName) && Objects.equals(datatype, indexHole.datatype)
                        && Objects.equals(dateRanges, indexHole.dateRanges);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldName, datatype, dateRanges);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", FieldIndexHole.class.getSimpleName() + "[", "]").add("fieldName='" + fieldName + "'").add("dataType='" + datatype + "'")
                        .add("dateRanges=" + dateRanges).toString();
    }
}

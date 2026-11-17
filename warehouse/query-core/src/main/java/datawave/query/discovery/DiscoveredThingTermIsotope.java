package datawave.query.discovery;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.MapWritable;

/**
 * A variant of DiscoveredThing in which equality is defined as comparing getTerm() only.
 */
public class DiscoveredThingTermIsotope extends DiscoveredThing {
    public DiscoveredThingTermIsotope(String term, String field, String type, String date, String columnVisibility, long count,
                    MapWritable countsByColumnVisibility) {
        super(term, field, type, date, columnVisibility, count, countsByColumnVisibility);

    }

    public DiscoveredThingTermIsotope(String term, String columnVisibility) {
        super(term, columnVisibility);
    }

    public DiscoveredThingTermIsotope() {
        super();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DiscoveredThingTermIsotope) {
            DiscoveredThingTermIsotope other = (DiscoveredThingTermIsotope) o;
            return new EqualsBuilder().append(getTerm(), other.getTerm()).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTerm()).toHashCode();
    }
}

package datawave.query.config;

import datawave.services.query.configuration.GenericQueryConfiguration;
import datawave.services.query.logic.BaseQueryLogic;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Range;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Thin wrapper around GenericQueryConfiguration for use by the {@link datawave.query.tables.content.ContentQueryTable}
 * 
 */
public class ContentQueryConfiguration extends GenericQueryConfiguration implements Serializable {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1662850178943683419L;
    
    private final Collection<Range> ranges = new TreeSet<>();
    
    public ContentQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
    }
    
    public void addRange(final Range range) {
        if (null != range) {
            synchronized (this.ranges) {
                this.ranges.add(range);
            }
        }
    }
    
    public Collection<Range> getRanges() {
        final Collection<Range> orderedCopy;
        synchronized (this.ranges) {
            orderedCopy = new ArrayList<>(this.ranges);
        }
        return orderedCopy;
    }
    
    public void setRanges(final Collection<Range> ranges) {
        // As a single atomic operation, clear the range and add all of the
        // specified ranges
        synchronized (this.ranges) {
            this.ranges.clear();
            if (null != ranges) {
                this.ranges.addAll(ranges);
            }
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        ContentQueryConfiguration that = (ContentQueryConfiguration) o;
        return Objects.equals(ranges, that.ranges);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ranges);
    }
}

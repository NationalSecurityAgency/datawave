package datawave.query.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Range;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;

/**
 * Thin wrapper around GenericQueryConfiguration for use by the {@link datawave.query.tables.content.ContentQueryTable}
 *
 */
public class ContentQueryConfiguration extends GenericQueryConfiguration {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1662850178943683419L;

    private Query query;
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

    public Query getQuery() {
        return query;
    }

    public Collection<Range> getRanges() {
        final Collection<Range> orderedCopy;
        synchronized (this.ranges) {
            orderedCopy = new ArrayList<>(this.ranges);
        }
        return orderedCopy;
    }

    public void setQuery(Query query) {
        this.query = query;
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
}

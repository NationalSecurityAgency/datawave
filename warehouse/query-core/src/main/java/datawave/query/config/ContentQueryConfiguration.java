package datawave.query.config;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.query.tables.content.ContentQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.data.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Thin wrapper around GenericQueryConfiguration for use by the {@link ContentQueryLogic}
 *
 */
public class ContentQueryConfiguration extends GenericQueryConfiguration implements Serializable {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1662850178943683419L;

    private transient Collection<Range> ranges;

    public ContentQueryConfiguration() {
        super();
        setQuery(new QueryImpl());
    }

    public ContentQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
        this.ranges = new TreeSet<>();
    }

    /**
     * Factory method that instantiates a fresh ContentQueryConfiguration
     *
     * @return - a clean ContentQueryConfiguration
     */
    public static ContentQueryConfiguration create() {
        return new ContentQueryConfiguration();
    }

    public synchronized void addRange(final Range range) {
        if (null != range) {
            this.ranges.add(range);
        }
    }

    public synchronized Collection<Range> getRanges() {
        return new ArrayList<>(this.ranges);
    }

    public synchronized void setRanges(final Collection<Range> ranges) {
        // As a single atomic operation, clear the range and add all of the
        // specified ranges
        this.ranges.clear();
        if (null != ranges) {
            this.ranges.addAll(ranges);
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

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(ranges != null ? ranges.size() : 0);
        for (Range range : ranges) {
            range.write(out);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        ranges = new TreeSet<>();
        int numRanges = in.readInt();
        while (numRanges-- > 0) {
            Range range = new Range();
            range.readFields(in);
            ranges.add(range);
        }
    }
}

package datawave.query.tables.content;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class ContentQueryCheckpoint extends QueryCheckpoint implements Serializable {

    private transient Collection<Range> ranges;

    public ContentQueryCheckpoint(QueryKey queryKey, Collection<Range> ranges) {
        super(queryKey, null);
        this.ranges = ranges;
    }

    public Collection<Range> getRanges() {
        return ranges;
    }

    @Override
    public String toString() {
        return getQueryKey() + ": " + getRanges();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof ContentQueryCheckpoint))
            return false;

        ContentQueryCheckpoint that = (ContentQueryCheckpoint) o;

        return new EqualsBuilder().appendSuper(super.equals(o)).append(ranges, that.ranges).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).appendSuper(super.hashCode()).append(ranges).toHashCode();
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
        int numRanges = in.readInt();
        if (numRanges > 0) {
            ranges = new ArrayList<>();
            while (numRanges-- > 0) {
                Range range = new Range();
                range.readFields(in);
                ranges.add(range);
            }
        }
    }
}

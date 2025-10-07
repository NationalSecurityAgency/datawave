package datawave.query.tables.keyword;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;

/** Enables checkpointing for the KeywordQueryLogic */
public class KeywordQueryCheckpoint extends QueryCheckpoint implements Serializable {

    private transient Collection<Range> ranges;

    public KeywordQueryCheckpoint(QueryKey queryKey, Collection<Range> ranges) {
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

        if (!(o instanceof KeywordQueryCheckpoint))
            return false;

        KeywordQueryCheckpoint that = (KeywordQueryCheckpoint) o;

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

package datawave.query.config;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class TermFrequencyQueryConfiguration extends GenericQueryConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Range range = null;

    public TermFrequencyQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        TermFrequencyQueryConfiguration that = (TermFrequencyQueryConfiguration) o;
        return Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), range);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeBoolean(range != null);
        if (range != null) {
            range.write(out);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (in.readBoolean()) {
            Range range = new Range();
            range.readFields(in);
            this.range = range;
        }
    }
}

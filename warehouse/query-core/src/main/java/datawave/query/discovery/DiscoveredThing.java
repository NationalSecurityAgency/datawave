package datawave.query.discovery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

public class DiscoveredThing implements WritableComparable<DiscoveredThing> {
    private String term, field, type, date, columnVisibility;
    private final VLongWritable count;
    private final MapWritable countsByColumnVisibility;

    public DiscoveredThing(String term, String field, String type, String date, String columnVisibility, long count, MapWritable countsByColumnVisibility) {
        this.term = term;
        this.field = field;
        this.type = type;
        this.date = date;
        this.columnVisibility = columnVisibility;
        this.count = new VLongWritable(count);
        this.countsByColumnVisibility = countsByColumnVisibility;
    }

    public DiscoveredThing() {
        count = new VLongWritable();
        countsByColumnVisibility = new MapWritable();
    }

    public String getTerm() {
        return term;
    }

    public String getField() {
        return field;
    }

    public String getType() {
        return type;
    }

    public String getDate() {
        return date;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public long getCount() {
        return count.get();
    }

    public MapWritable getCountsByColumnVisibility() {
        return countsByColumnVisibility;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(term);
        out.writeUTF(field);
        out.writeUTF(type);
        out.writeUTF(date);
        out.writeUTF(columnVisibility);
        count.write(out);
        countsByColumnVisibility.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term = in.readUTF();
        field = in.readUTF();
        type = in.readUTF();
        date = in.readUTF();
        columnVisibility = in.readUTF();
        count.readFields(in);
        countsByColumnVisibility.readFields(in);
    }

    @Override
    public int compareTo(DiscoveredThing o) {
        CompareToBuilder cmp = new CompareToBuilder();
        if (o == null) {
            return 1;
        } else {
            cmp.append(getTerm(), o.getTerm());
            cmp.append(getField(), o.getField());
            cmp.append(getType(), o.getType());
            cmp.append(getDate(), o.getDate());
            cmp.append(getColumnVisibility(), o.getColumnVisibility());
            cmp.append(getCount(), o.getCount());
            return cmp.toComparison();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DiscoveredThing))
            return false;
        DiscoveredThing other = (DiscoveredThing) o;
        return Objects.equal(getTerm(), other.getTerm()) && Objects.equal(getField(), other.getField()) && Objects.equal(getType(), other.getType())
                        && Objects.equal(getDate(), other.getDate()) && Objects.equal(getColumnVisibility(), other.getColumnVisibility())
                        && Objects.equal(getCount(), other.getCount());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getTerm(), getField(), getType(), getDate(), getColumnVisibility(), getCount());
    }

    @Override
    public String toString() {
        return "DiscoveredThing [term=" + term + ", field=" + field + ", type=" + type + ", date=" + date + ", columnVisibility=" + columnVisibility
                        + ", count=" + count + "]";
    }
}

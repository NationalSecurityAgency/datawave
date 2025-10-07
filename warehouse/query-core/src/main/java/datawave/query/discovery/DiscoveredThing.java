package datawave.query.discovery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

import datawave.core.query.configuration.ResultContext;

public class DiscoveredThing implements WritableComparable<DiscoveredThing> {
    private ResultContext context;
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
            cmp.append(getCountsByColumnVisibility(), o.getCountsByColumnVisibility());
            return cmp.toComparison();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiscoveredThing that = (DiscoveredThing) o;
        return Objects.equals(term, that.term) && Objects.equals(field, that.field) && Objects.equals(type, that.type) && Objects.equals(date, that.date)
                        && Objects.equals(columnVisibility, that.columnVisibility) && Objects.equals(count, that.count)
                        && Objects.equals(countsByColumnVisibility, that.countsByColumnVisibility);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, field, type, date, columnVisibility, count, countsByColumnVisibility);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DiscoveredThing.class.getSimpleName() + "[", "]").add("term='" + term + "'").add("field='" + field + "'")
                        .add("type='" + type + "'").add("date='" + date + "'").add("columnVisibility='" + columnVisibility + "'").add("count=" + count)
                        .add("countsByColumnVisibility=" + countsByColumnVisibility).toString();
    }
}

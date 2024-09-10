package datawave.metrics.analytic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Objects;

public class Phase implements WritableComparable<Phase> {
    private String name;
    private long start, end;

    public Phase() {}

    public Phase(String name, long start, long end) {
        this.name = name;
        this.start = start;
        this.end = end;
    }

    public String name() {
        return name;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        VLongWritable vlaw = new VLongWritable();
        vlaw.set(start);
        vlaw.write(out);
        vlaw.set(end);
        vlaw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        VLongWritable vlaw = new VLongWritable();
        vlaw.readFields(in);
        start = vlaw.get();
        vlaw.readFields(in);
        end = vlaw.get();
    }

    @Override
    public int compareTo(Phase o) {
        int diff = name.compareTo(o.name);
        if (diff == 0) {
            long ldiff = Long.compare(start, o.start);
            if (ldiff == 0) {
                ldiff = Long.compare(end, o.end);
                if (ldiff == 0) {
                    return 0;
                } else {
                    return ldiff > 0 ? 1 : -1;
                }
            } else {
                return ldiff > 0 ? 1 : -1;
            }
        } else {
            return diff;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Phase)
            return this.compareTo((Phase) o) == 0;
        else
            return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, start, end);
    }

    public String toString() {
        return "[" + name + "," + new Date(start) + "," + new Date(end) + "]";
    }
}

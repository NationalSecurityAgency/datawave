package datawave.mr.bulk.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Lists;

public class FileRangeSplit extends InputSplit implements Writable {

    protected List<Range> ranges = Lists.newArrayList();

    private Path file;
    private long start;
    private long length;
    private String[] hosts;

    public FileRangeSplit() {

    }

    public FileRangeSplit(Collection<Range> rangeColl, Path file, long start, long length, String[] hosts) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.ranges.addAll(rangeColl);
    }

    public FileRangeSplit(Range range, Path file, long start, long length, String[] hosts) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.ranges.add(range);
    }

    public Range getRange() {
        return ranges.get(0);
    }

    public List<Range> getRanges() {
        return Collections.unmodifiableList(ranges);
    }

    public boolean isSame(FileRangeSplit other) {
        return file.equals(other.file) && start == other.start && length == other.length;
    }

    /** The file containing this split's data. */
    public Path getPath() {
        return file;
    }

    /** The position of the first byte in the file to process. */
    public long getStart() {
        return start;
    }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return file + ":" + start + "+" + length;
    }

    // //////////////////////////////////////////
    // Writable methods
    // //////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(start);
        out.writeLong(length);
        out.writeInt(ranges.size());
        for (Range range : ranges) {
            range.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        start = in.readLong();
        length = in.readLong();
        hosts = null;
        int numberRanges = in.readInt();
        for (int i = 0; i < numberRanges; i++) {
            Range range = new Range();
            range.readFields(in);
            ranges.add(range);
        }

    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[] {};
        } else {
            return this.hosts;
        }
    }

}

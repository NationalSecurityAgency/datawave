package datawave.mr.bulk.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * The Class RangeSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
 */
public class RangeSplit extends InputSplit implements Writable {

    protected SortedSet<Range> ranges;

    protected Key startKey = null;
    protected Key endKey = null;

    protected String[] locations;
    /**
     * my split strategy
     */
    protected LocationStrategy strategy = new DefaultLocationStrategy();

    public RangeSplit() {
        ranges = Sets.newTreeSet();
        locations = new String[0];
    }

    public RangeSplit(LocationStrategy strategy) {
        this();
        this.strategy = strategy;
    }

    public Collection<Range> getRanges() {
        return ranges;
    }

    private int startByte = 0;
    private int endByte = 0;
    private long range = 0;
    private PartialKey depth = PartialKey.ROW;

    /**
     * This is called with the endKey and startKey have changed. Determine which part of the keys to compare, and then determine which bytes therein to compare.
     * This will set the startByte, endByte, range, and depth class members.
     */
    private void updateProgressDepth() {
        this.startByte = this.endByte = 0;
        this.range = 0;
        if (startKey.compareTo(this.endKey, PartialKey.ROW) != 0) {
            this.depth = PartialKey.ROW;
            updateSignificantBytes(this.startKey.getRowData(), this.endKey.getRowData());
        } else if (this.startKey.compareTo(this.endKey, PartialKey.ROW_COLFAM) != 0) {
            this.depth = PartialKey.ROW_COLFAM;
            updateSignificantBytes(this.startKey.getColumnFamilyData(), this.endKey.getColumnFamilyData());
        } else if (this.startKey.compareTo(this.endKey, PartialKey.ROW_COLFAM_COLQUAL) != 0) {
            this.depth = PartialKey.ROW_COLFAM_COLQUAL;
            updateSignificantBytes(this.startKey.getColumnQualifierData(), this.endKey.getColumnQualifierData());
        } else {
            this.depth = PartialKey.ROW_COLFAM_COLQUAL_COLVIS;
        }
    }

    /**
     * Given the start and end of the range, determine a set of bytes therein that are significant enough to provide a reasonable range of progress values. This
     * will set the startByte, endByte, and range class members.
     *
     * @param start
     * @param end
     */
    private void updateSignificantBytes(ByteSequence start, ByteSequence end) {
        // we want to compare enough bytes that we get a resolution of .1 percent or less
        int startLen = start.length();
        int endLen = end.length();
        int maxDepth = Math.max(startLen, endLen);
        long startVal = 0;
        long endVal = 0;
        this.startByte = 0;
        for (int i = 0; i < maxDepth; i++) {
            // get the start and end byte values as unsigned ints
            int startByteVal = (i < startLen ? (start.byteAt(i) & 0xFF) : 0);
            int endByteVal = (i < endLen ? (end.byteAt(i) & 0xFF) : 0);
            // Add this these values in base 256
            startVal = (startVal * 256) + startByteVal;
            endVal = (endVal * 256) + endByteVal;

            if (startVal == endVal) {
                // the bytes match up until now, up the startByte and continue
                startVal = endVal = 0;
                this.startByte = i + 1;
            } else if (endVal - startVal >= 1000) {
                // we got a reasonable range of bytes to compare, remember the range and return
                this.endByte = i + 1;
                this.range = (endVal - startVal);
                return;
            }
        }
        this.endByte = maxDepth;
        this.range = (endVal - startVal);
    }

    /**
     * Determine the progress the end is from the start relative to the overall range. This will use the startByte/endByte range of bytes to calculate the
     * distance. Then the distance is compared to the range to return the overall percentage.
     *
     * @param start
     * @param end
     * @return the progress [0.0, 1.0]
     */
    public float getProgress(ByteSequence start, ByteSequence end) {
        int startLen = start.length();
        int endLen = end.length();
        long startVal = 0;
        long endVal = 0;
        for (int i = this.startByte; i < this.endByte; i++) {
            int startByteVal = (i < startLen ? (start.byteAt(i) & 0xFF) : 0);
            int endByteVal = (i < endLen ? (end.byteAt(i) & 0xFF) : 0);
            startVal = (startVal * 256) + startByteVal;
            endVal = (endVal * 256) + endByteVal;
        }
        return (float) (endVal - startVal) / this.range;
    }

    /**
     * Determine the progress the current key is from the start key relative to the overall range. This will use the depth to determine which part of the key to
     * use, and then use the method above to get the actuall progress value.
     *
     * @param currentKey
     * @return the progress [0.0, 1.0]
     */
    public float getProgress(Key currentKey) {
        if (currentKey == null)
            return 0f;

        // if this.range > 0, then we must have a startKey and endKey
        if (this.range > 0) {
            if (depth == PartialKey.ROW) {
                // just look at the row progress
                return getProgress(startKey.getRowData(), currentKey.getRowData());
            } else if (depth == PartialKey.ROW_COLFAM) {
                // just look at the column family progress
                return getProgress(startKey.getColumnFamilyData(), currentKey.getColumnFamilyData());
            } else if (depth == PartialKey.ROW_COLFAM_COLQUAL) {
                // just look at the column qualifier progress
                return getProgress(startKey.getColumnQualifierData(), currentKey.getColumnQualifierData());
            }
        }

        // if we can't figure it out, then claim no progress
        return 0f;
    }

    public RangeSplit(LocationStrategy strategy, String table, String[] locations) {
        this(strategy);
        this.ranges = Sets.newTreeSet(ranges);
        this.locations = locations;
    }

    public void addRanges(Collection<Range> ranges) {
        for (Range range : ranges) {
            addRange(range);
        }
    }

    protected void addRange(Range range) {
        if (null == startKey) {
            startKey = range.getStartKey();
        } else {
            if (range.getStartKey().compareTo(startKey) < 0) {
                startKey = range.getStartKey();
            }
        }

        if (null == endKey) {
            endKey = range.getEndKey();
        } else {
            if (range.getEndKey().compareTo(endKey) > 0) {
                endKey = range.getEndKey();
            }
        }

        updateProgressDepth();

        this.ranges.add(range);
    }

    /**
     * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
     */
    public long getLength() throws IOException {
        Range firstRange = ranges.first();
        Range listRange = ranges.last();
        Text startRow = firstRange.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : firstRange.getStartKey().getRow();
        Text stopRow = listRange.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : listRange.getEndKey().getRow();
        int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
        long diff = 0;

        byte[] start = startRow.getBytes();
        byte[] stop = stopRow.getBytes();
        for (int i = 0; i < maxCommon; ++i) {
            diff |= 0xff & (start[i] ^ stop[i]);
            diff <<= Byte.SIZE;
        }

        if (startRow.getLength() != stopRow.getLength())
            diff |= 0xff;

        return diff + 1;
    }

    public String[] getLocations() throws IOException {
        return locations;
    }

    public void readFields(DataInput in) throws IOException {
        ranges = Sets.newTreeSet();
        int numLocs = in.readInt();
        for (int i = 0; i < numLocs; ++i) {
            Range range = new Range();
            range.readFields(in);
            ranges.add(range);
        }
        numLocs = in.readInt();
        locations = new String[numLocs];
        for (int i = 0; i < numLocs; ++i)
            locations[i] = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(ranges.size());
        for (Range range : ranges)
            range.write(out);
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; ++i)
            out.writeUTF(locations[i]);
    }

    @Override
    public Object clone() {
        return new RangeSplit(strategy, null, locations);
    }

    @Override
    public int hashCode() {
        HashFunction hf = Hashing.goodFastHash(64);
        return hf.newHasher().putObject(this, strategy).hash().asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RangeSplit) {
            return Arrays.equals(((RangeSplit) obj).locations, locations);
        } else
            return false;
    }

    public Key getStartKey() {
        return startKey;
    }

    public Key getEndKey() {
        return endKey;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(startKey).append(endKey).append(ranges);

        return builder.toString();
    }
}

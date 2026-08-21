package datawave.ingest.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// For a file with N blocks
// startBlock 0 - beginning of the file to the first block
// startBlock N - last block to the end
public class RFileSplit extends FileSplit {
    private long startBlock;
    private long numBlocks;
    private Key top;

    // The seek range boundaries, when they were resolved while creating the split, so that reading it needs no index traversal
    private boolean boundsKnown;
    private Key startKey;
    private Key endKey;

    public RFileSplit() {
        top = new Key();
    }

    public RFileSplit(Path path, long fileStart, long fileLength, String[] hosts, long startBlock, long numBlocks, Key top) {
        super(path, fileStart, fileLength, hosts);
        this.startBlock = startBlock;
        this.numBlocks = numBlocks;
        this.top = top;
    }

    public RFileSplit(Path path, long fileStart, long fileLength, String[] hosts, long startBlock, long numBlocks, Key top, Key startKey, Key endKey) {
        this(path, fileStart, fileLength, hosts, startBlock, numBlocks, top);
        this.boundsKnown = true;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public long getStartBlock() {
        return startBlock;
    }

    public long getNumBlocks() {
        return numBlocks;
    }

    public Key getTopKey() {
        return top;
    }

    /**
     * Whether the seek range was resolved while creating this split, which lets a reader seek straight to it instead of walking the index.
     *
     * @return true if {@link #getSeekRange()} may be used
     */
    public boolean hasSeekRange() {
        return boundsKnown;
    }

    /**
     * The range this split covers, resolved when the split was created
     *
     * @return
     */
    public Range getSeekRange() {
        return getSeekRange(startKey, endKey);
    }

    /**
     * Create a range which is start key inclusive and end key exclusive
     *
     * @param start
     * @param end
     * @return
     */
    public Range getSeekRange(Key start, Key end) {
        return new Range(start, true, end, false);
    }

    @Override
    public String toString() {
        return super.toString() + ":" + startBlock + "+" + numBlocks + "+" + top;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        top.write(out);
        out.writeLong(startBlock);
        out.writeLong(numBlocks);
        out.writeBoolean(boundsKnown);
        if (boundsKnown) {
            writeKey(out, startKey);
            writeKey(out, endKey);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        top.readFields(in);
        startBlock = in.readLong();
        numBlocks = in.readLong();
        boundsKnown = in.readBoolean();
        if (boundsKnown) {
            startKey = readKey(in);
            endKey = readKey(in);
        }
    }

    private static void writeKey(DataOutput out, Key key) throws IOException {
        out.writeBoolean(key != null);
        if (key != null) {
            key.write(out);
        }
    }

    private static Key readKey(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        Key key = new Key();
        key.readFields(in);
        return key;
    }
}

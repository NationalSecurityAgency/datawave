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

    public RFileSplit() {
        top = new Key();
    }

    public RFileSplit(Path path, long fileStart, long fileLength, String[] hosts, int startBlock, int numBlocks, Key top) {
        super(path, fileStart, fileLength, hosts);
        this.startBlock = startBlock;
        this.numBlocks = numBlocks;
        this.top = top;
    }

    public long getStartBlock() {
        return startBlock;
    }

    public Key getTopKey() {
        return top;
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
    public long getLength() {
        return numBlocks;
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
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        top.readFields(in);
        startBlock = in.readLong();
        numBlocks = in.readLong();
    }
}

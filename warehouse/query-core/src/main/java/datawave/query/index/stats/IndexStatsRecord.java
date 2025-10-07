package datawave.query.index.stats;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Objects;

/**
 * See IndexStatsJob.java for how this fits into the index stats job.
 */
public class IndexStatsRecord implements WritableComparable<IndexStatsRecord> {
    private final VLongWritable numberUnique;
    private final VLongWritable count;
    private final FloatWritable averageWordLength;

    public IndexStatsRecord() {
        numberUnique = new VLongWritable();
        count = new VLongWritable();
        averageWordLength = new FloatWritable();
    }

    @Override
    public int compareTo(IndexStatsRecord o) {
        int result = numberUnique.compareTo(o.numberUnique);
        if (0 == result) {
            result = count.compareTo(o.count);
        }
        if (0 == result) {
            result = averageWordLength.compareTo(o.averageWordLength);
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(numberUnique, count, averageWordLength);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IndexStatsRecord)) {
            return false;
        }
        final IndexStatsRecord otherData = (IndexStatsRecord) o;
        return Objects.equal(numberUnique, otherData.numberUnique) && Objects.equal(count, otherData.count)
                        && Objects.equal(averageWordLength, otherData.averageWordLength);
    }

    public byte[] toByteArray() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            DataOutputStream dataOutput = new DataOutputStream(baos);
            write(dataOutput);
            return baos.toByteArray();
        } finally {
            baos.close();
        }
    }

    @Override
    public String toString() {
        return "\"unique\": " + Long.toString(this.getNumberOfUniqueWords().get()) + ", \"count\": " + Long.toString(this.getWordCount().get())
                        + ", \"averageLength\": " + Float.toString(this.getAverageWordLength().get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numberUnique.write(dataOutput);
        count.write(dataOutput);
        averageWordLength.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numberUnique.readFields(dataInput);
        count.readFields(dataInput);
        averageWordLength.readFields(dataInput);
    }

    public VLongWritable getNumberOfUniqueWords() {
        return numberUnique;
    }

    public void setNumberOfUniqueWords(long numUnique) {
        numberUnique.set(numUnique);
    }

    public VLongWritable getWordCount() {
        return count;
    }

    public void setWordCount(long sum) {
        count.set(sum);
    }

    public FloatWritable getAverageWordLength() {
        return averageWordLength;
    }

    public void setAverageWordLength(float wordCount) {
        averageWordLength.set(wordCount);
    }
}

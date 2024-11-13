package datawave.metrics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * A generic base class for combing sequence files for input into map jobs. For any given {@code <K,V>} pair of types, this logic is the same.
 *
 * @param <K>
 *            type for the key
 * @param <V>
 *            type for the value
 */
public class CombinedSequenceFileRecordReader<K,V> extends SequenceFileRecordReader<K,V> {
    private int index;

    public CombinedSequenceFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
        this.index = index;
        initialize(split, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit fSplit = (CombineFileSplit) split;
        super.initialize(new FileSplit(fSplit.getPath(index), 0, fSplit.getLength(index), fSplit.getLocations()), context);
    }
}

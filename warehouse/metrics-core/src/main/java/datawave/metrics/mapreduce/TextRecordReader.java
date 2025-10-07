package datawave.metrics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * A Text/Counters implementation of CombinedSequenceFileRecordReader
 *
 */
public class TextRecordReader extends CombinedSequenceFileRecordReader<Text,Counters> {

    public TextRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
        super(split, context, index);
    }

}

package datawave.metrics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * A specification of CSFRR that uses NullWritable as the key and Counters as the value. This is necessary because the InputFormat requires a concrete class
 * that does not use generics.
 *
 */
public class NullRecordReader extends CombinedSequenceFileRecordReader<NullWritable,Counters> {

    public NullRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
        super(split, context, index);
    }

}

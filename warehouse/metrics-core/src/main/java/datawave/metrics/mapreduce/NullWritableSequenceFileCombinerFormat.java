package datawave.metrics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * An implementation of CombineFileInputFormat that can read NullWritable/Counters pairs from sequence files.
 *
 */
public class NullWritableSequenceFileCombinerFormat extends CombineFileInputFormat<NullWritable,Counters> {

    @Override
    public RecordReader<NullWritable,Counters> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, NullRecordReader.class);
    }

}

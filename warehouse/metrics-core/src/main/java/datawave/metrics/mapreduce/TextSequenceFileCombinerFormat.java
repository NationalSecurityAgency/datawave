package datawave.metrics.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * An implementation of CombineFileInputFormat that can read Text/Counters pairs from sequence files.
 *
 */
public class TextSequenceFileCombinerFormat extends CombineFileInputFormat<Text,Counters> {

    @Override
    public RecordReader<Text,Counters> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, TextRecordReader.class);
    }

}

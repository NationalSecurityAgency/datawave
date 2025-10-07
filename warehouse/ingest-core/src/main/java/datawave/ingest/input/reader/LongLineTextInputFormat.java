package datawave.ingest.input.reader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A copy of {@link TextInputFormat} which uses a {@link LongLineEventRecordReader} instead of a {@link LineRecordReader}.
 */
public class LongLineTextInputFormat extends FileInputFormat<LongWritable,Text> {

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new LongLineEventRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}

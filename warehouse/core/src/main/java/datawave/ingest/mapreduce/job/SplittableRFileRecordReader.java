package datawave.ingest.mapreduce.job;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SplittableRFileRecordReader extends RFileRecordReader {
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof SplittableRFileInputFormat.RFileSplit) {
            fileIterator = SplittableRFileInputFormat.getIterator(context.getConfiguration(), (SplittableRFileInputFormat.RFileSplit) split);
        } else {
            super.initialize(split, context);
        }
    }
}

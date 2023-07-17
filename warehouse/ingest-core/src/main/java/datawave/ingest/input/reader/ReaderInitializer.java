package datawave.ingest.input.reader;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface ReaderInitializer {

    void initialize(InputSplit split, TaskAttemptContext context) throws IOException;

}

package datawave.ingest.input.reader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public interface ReaderInitializer {

    void initialize(InputSplit split, TaskAttemptContext context) throws IOException;

}

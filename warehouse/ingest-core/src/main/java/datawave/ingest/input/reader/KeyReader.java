package datawave.ingest.input.reader;

import org.apache.hadoop.io.LongWritable;

public interface KeyReader {

    LongWritable readKey();

}

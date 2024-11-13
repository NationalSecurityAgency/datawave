package datawave.ingest.input.reader;

import org.apache.hadoop.io.Text;

public interface ValueReader {

    Text readValue();

}

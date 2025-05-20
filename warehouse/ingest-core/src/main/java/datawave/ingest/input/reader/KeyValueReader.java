package datawave.ingest.input.reader;

import java.io.IOException;

public interface KeyValueReader {

    boolean readKeyValue() throws IOException;

}

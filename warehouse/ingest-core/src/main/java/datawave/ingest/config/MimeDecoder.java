package datawave.ingest.config;

import java.io.IOException;

public interface MimeDecoder {

    byte[] decode(byte[] b) throws IOException;

}

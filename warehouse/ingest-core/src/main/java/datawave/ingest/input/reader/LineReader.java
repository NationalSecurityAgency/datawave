package datawave.ingest.input.reader;

public interface LineReader {

    class Properties {

        public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";
        public static final String LONGLINE_NEWLINE_INCLUDED = "longline.newline.included";
        public static final String MAPRED_LONGLINE_READER_MAXLENGTH = "mapred.linerecordreader.maxlength";

    }
}

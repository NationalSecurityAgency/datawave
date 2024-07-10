package datawave.query.util.sortedmap.rfile;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.byteDocumentToKeyValue;

public class RFileByteDocumentOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedSetOutputStream<Map.Entry<byte[],Document>> {
    public RFileByteDocumentOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Map.Entry<byte[],Document> obj) throws IOException {
        writeKeyValue(byteDocumentToKeyValue(obj));
    }
}

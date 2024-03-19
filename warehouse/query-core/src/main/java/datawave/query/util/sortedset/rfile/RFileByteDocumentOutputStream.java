package datawave.query.util.sortedset.rfile;

import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.byteDocumentToKeyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import datawave.query.attributes.Document;
import datawave.query.util.sortedset.FileSortedSet;

public class RFileByteDocumentOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedSet.SortedSetOutputStream<Map.Entry<byte[],Document>> {
    public RFileByteDocumentOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Map.Entry<byte[],Document> obj) throws IOException {
        writeKeyValue(byteDocumentToKeyValue(obj));
    }
}

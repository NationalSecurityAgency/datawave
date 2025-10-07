package datawave.query.util.sortedmap.rfile;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.byteToKey;
import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.documentToValue;

import java.io.IOException;
import java.io.OutputStream;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;

public class RFileByteDocumentOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedMapOutputStream<byte[],Document> {
    public RFileByteDocumentOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(byte[] k, Document v) throws IOException {
        writeKeyValue(byteToKey(k), documentToValue(v));
    }
}

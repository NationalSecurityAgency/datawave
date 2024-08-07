package datawave.query.util.sortedmap.rfile;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.byteDocumentToKeyValue;
import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.byteToKey;
import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.documentToValue;

public class RFileByteDocumentOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedMapOutputStream<byte[],Document> {
    public RFileByteDocumentOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(byte[] k, Document v) throws IOException {
        writeKeyValue(byteToKey(k), documentToValue(v));
    }
}

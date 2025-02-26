package datawave.query.util.sortedmap.rfile;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.documentToValue;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;

public class RFileKeyDocumentOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedMapOutputStream<Key,Document> {
    public RFileKeyDocumentOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Key k, Document v) throws IOException {
        writeKeyValue(k, documentToValue(v));
    }
}

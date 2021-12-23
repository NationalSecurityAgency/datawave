package datawave.query.tables.serialization;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class SerializedDocument implements SerializedDocumentIfc{

    private final Document doc;

    private Key key;

    public SerializedDocument(Document doc, Key key){
        this.doc=doc;
        this.key=key;
    }

    public SerializedDocument(Document doc){
        this(doc,null);
    }
    @Override
    public Key computeKey() {
        return this.doc.isMetadataSet() ? this.doc.getMetadata() : key;
    }

    @Override
    public <T> T get() {
        return (T)doc;
    }

    @Override
    public Document getAsDocument(){
        return doc;
    }

    private byte[] getBytes(Key key) {
        ByteSequence row = key.getRowData();
        ByteSequence cf = key.getColumnFamilyData();

        // only append the last 2 tokens (the datatype and uid)
        // we are expecting that they may be prefixed with a count (see sortedUIDs in the DefaultQueryPlanner / QueryIterator)
        int nullCount = 0;
        int index = -1;
        for (int i = 0; i < cf.length() && nullCount < 2; i++) {
            if (cf.byteAt(i) == 0) {
                nullCount++;
                if (index == -1) {
                    index = i;
                }
            }
        }
        int dataTypeOffset = index + 1;
        int offset = cf.offset() + dataTypeOffset;
        int length = cf.length() - dataTypeOffset;

        byte[] bytes = new byte[row.length() + length + 1];
        System.arraycopy(row.getBackingArray(), row.offset(), bytes, 0, row.length());
        System.arraycopy(cf.getBackingArray(), offset, bytes, row.length() + 1, length);
        return bytes;
    }
    @Override
    public byte[] getIdentifier() {
        Key key = computeKey();
        if (null != key){
            return getBytes(key);
        }
        return new byte[0];
    }

    @Override
    public int compareTo(SerializedDocumentIfc other) {
        if (other instanceof SerializedDocument){
            return this.doc.compareTo( ((SerializedDocument) other).doc);
        }
        return -1;
    }

    @Override
    public long size() {
        return doc.sizeInBytes();
    }
}

package datawave.query.function.serializer;

import com.google.common.collect.Maps;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.nio.ByteBuffer;
import java.util.Map;


/**
 * Document serialization that emphasizes converting metadata to JSON.
 */
public abstract class JsonMetadataSerializer extends DocumentSerializer{

    public JsonMetadataSerializer(boolean reducedResponse, boolean allowCompression) {
        super(reducedResponse, allowCompression);
    }


    @Override
    public Map.Entry<Key, Value> apply(Map.Entry<Key, Document> from) {

        final byte[] bytes = serialize(from.getValue());

        final Value v = getValue(from.getKey(),bytes);

        return Maps.immutableEntry(from.getKey(), v);
    }

    byte[] computeIdentifier(Key key) {
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

        int rowlen = row.length()- (int)4-row.offset();

        byte[] bytes = new byte[rowlen + length + 1];

        System.arraycopy(row.getBackingArray(), row.offset()+4, bytes, 0, rowlen);
        System.arraycopy(cf.getBackingArray(), offset, bytes, rowlen , length);
        return bytes;
    }

    public static int getDataLength(byte [] doc, int offset){
        ByteBuffer buf = ByteBuffer.wrap(doc,offset +(int)3,(int)4);
        return buf.getInt();
    }

    public static byte[] getIdentifier(byte[] doc, int offset,int docSize, int dataLength) {
        if (doc.length <= 7 || dataLength == 0){
            // if we don't have a document then we will return an empty identifier
            return new byte[0];
        }
        int totalLen = docSize - dataLength;
        totalLen-=(int)7;
        byte [] array = new byte [ totalLen ];

        System.arraycopy(doc,offset+(int)7+dataLength,array,0,totalLen);
        return array;
    }





    protected Value getValue(Key key, byte[] document) {
        byte[] header;
        byte[] identifier;
        byte[] dataToWrite;

        // Only compress the data if it's greater than minCompressionSize in size (bytes)
        if (DocumentSerialization.NONE != this.compression && document.length > minCompressionSize) {
            header = DocumentSerialization.getHeader(compression);
            dataToWrite = DocumentSerialization.writeBody(document, this.compression);

        } else {
            header = DocumentSerialization.getHeader();
            dataToWrite = document;
        }
        identifier = computeIdentifier(key); // used for computing the size

        int totalSize = identifier.length  + header.length + dataToWrite.length;
        totalSize+=(int)4;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(header);
        // writes 4 bytes
        buf.putInt(dataToWrite.length);
        buf.put(dataToWrite);
        buf.put(identifier);
        return new Value(buf.array());
    }
}

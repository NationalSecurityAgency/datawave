package datawave.query.tables.serialization;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

public interface SerializedDocumentIfc {

    Key computeKey();

    <T> T get();

    Document getAsDocument();

    byte [] getIdentifier();

    int compareTo(SerializedDocumentIfc other);

    long size();
}

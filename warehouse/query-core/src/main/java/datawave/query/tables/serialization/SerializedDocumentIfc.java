package datawave.query.tables.serialization;

import org.apache.accumulo.core.data.Key;

public interface SerializedDocumentIfc {

    Key computeKey();

    <T> T getAs(Class<?> as);

    int compareTo(SerializedDocumentIfc other);

    long size();
}

package datawave.core.ingest.data.tokenize;

import org.apache.lucene.util.Attribute;

public interface TruncateAttribute extends Attribute {
    boolean isTruncated();

    int getOriginalLength();

    void setTruncated(boolean truncated);

    void setOriginalLength(int length);
}

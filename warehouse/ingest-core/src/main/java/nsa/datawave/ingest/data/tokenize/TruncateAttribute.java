package nsa.datawave.ingest.data.tokenize;

import org.apache.lucene.util.Attribute;

public interface TruncateAttribute extends Attribute {
    public boolean isTruncated();
    
    public int getOriginalLength();
    
    public void setTruncated(boolean truncated);
    
    public void setOriginalLength(int length);
}

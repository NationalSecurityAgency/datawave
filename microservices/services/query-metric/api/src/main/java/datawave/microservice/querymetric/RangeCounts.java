package datawave.microservice.querymetric;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class RangeCounts implements Serializable {
    
    private long documentRangeCount;
    private long shardRangeCount;
    
    public long getDocumentRangeCount() {
        return documentRangeCount;
    }
    
    public long getShardRangeCount() {
        return shardRangeCount;
    }
    
    public void setDocumentRangeCount(long newDocumentRangeCount) {
        this.documentRangeCount = newDocumentRangeCount;
    }
    
    public void setShardRangeCount(long newShardRangeCount) {
        this.shardRangeCount = newShardRangeCount;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        RangeCounts that = (RangeCounts) o;
        
        return new EqualsBuilder().append(documentRangeCount, that.documentRangeCount).append(shardRangeCount, that.shardRangeCount).isEquals();
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(documentRangeCount, shardRangeCount);
    }
}

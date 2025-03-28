package datawave.microservice.querymetric;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricSummary implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final float MS_PER_S = 1000f;
    
    @XmlElement(name = "QueryCount")
    private long queryCount = 0L;
    @XmlElement(name = "MinimumPageResponseTime")
    private long minPageResponseTime = 0L;
    @XmlElement(name = "TotalPageResponseTime")
    private long totalPageResponseTime = 0L;
    @XmlElement(name = "TotalPages")
    private long totalPages = 0L;
    @XmlElement(name = "MaximumPageResponseTime")
    private long maxPageResponseTime = 0L;
    @XmlElement(name = "MinimumPageResultSize")
    private long minPageResultSize = 0L;
    @XmlElement(name = "TotalPageResultSize")
    private long totalPageResultSize = 0L;
    @XmlElement(name = "MaximumPageResultSize")
    private long maxPageResultSize = 0;
    
    public long getQueryCount() {
        return queryCount;
    }
    
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }
    
    public long getMinPageResponseTime() {
        return minPageResponseTime;
    }
    
    public void setMinPageResponseTime(long minPageResponseTime) {
        this.minPageResponseTime = minPageResponseTime;
    }
    
    public long getTotalPageResponseTime() {
        return totalPageResponseTime;
    }
    
    public void setTotalPageResponseTime(long totalPageResponseTime) {
        this.totalPageResponseTime = totalPageResponseTime;
    }
    
    public long getTotalPages() {
        return totalPages;
    }
    
    public void setTotalPages(long totalPages) {
        this.totalPages = totalPages;
    }
    
    public long getMaxPageResponseTime() {
        return maxPageResponseTime;
    }
    
    public void setMaxPageResponseTime(long maxPageResponseTime) {
        this.maxPageResponseTime = maxPageResponseTime;
    }
    
    public long getMinPageResultSize() {
        return minPageResultSize;
    }
    
    public void setMinPageResultSize(long minPageResultSize) {
        this.minPageResultSize = minPageResultSize;
    }
    
    public long getTotalPageResultSize() {
        return totalPageResultSize;
    }
    
    public void setTotalPageResultSize(long totalPageResultSize) {
        this.totalPageResultSize = totalPageResultSize;
    }
    
    public long getMaxPageResultSize() {
        return maxPageResultSize;
    }
    
    public void setMaxPageResultSize(long maxPageResultSize) {
        this.maxPageResultSize = maxPageResultSize;
    }
    
    public float getAvgPageResultSize() {
        if (0 == this.totalPages)
            return 0;
        return this.totalPageResultSize / this.totalPages;
    }
    
    public float getAvgPageResponseTime() {
        if (0 == this.totalPages)
            return 0;
        return this.totalPageResponseTime / this.totalPages;
    }
    
    public float getAveragePagesTime() {
        if (0 == getTotalPageResponseTime()) {
            return 0;
        }
        return (float) getTotalPages() / (float) getTotalPageResponseTime();
    }
    
    public float getAveragePagesPerSecond() {
        return getAveragePagesTime() * MS_PER_S;
    }
    
    public float getAverageResultsTime() {
        if (0 == getTotalPageResponseTime()) {
            return 0;
        }
        return (float) getTotalPageResultSize() / (float) getTotalPageResponseTime();
    }
    
    public float getAverageResultsPerSecond() {
        return getAverageResultsTime() * MS_PER_S;
    }
    
    public void addQuery() {
        if (this.queryCount == Long.MAX_VALUE)
            return;
        this.queryCount++;
    }
    
    public void addPage(long pagesize, long responseTime) {
        this.maxPageResultSize = Math.max(this.maxPageResultSize, pagesize);
        this.minPageResultSize = Math.min(this.minPageResultSize, pagesize);
        if (this.totalPageResultSize != Long.MAX_VALUE) {
            this.totalPageResultSize += pagesize;
            if (this.totalPageResultSize < 0)
                this.totalPageResultSize = Long.MAX_VALUE;
        }
        
        this.maxPageResponseTime = Math.max(this.maxPageResponseTime, responseTime);
        this.minPageResponseTime = Math.min(this.minPageResponseTime, responseTime);
        if (this.totalPageResponseTime != Long.MAX_VALUE) {
            this.totalPageResponseTime += responseTime;
            if (this.totalPageResponseTime < 0)
                this.totalPageResponseTime = Long.MAX_VALUE;
        }
        
        this.totalPages++;
        
    }
    
}

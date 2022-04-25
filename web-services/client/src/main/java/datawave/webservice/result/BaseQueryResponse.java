package datawave.webservice.result;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class BaseQueryResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "LogicName")
    private String logicName = null;
    
    @XmlElement(name = "QueryId")
    private String queryId = null;
    
    @XmlElement(name = "PageNumber")
    private long pageNumber = 0;
    
    @XmlElement(name = "PartialResults")
    private boolean partialResults = false;
    
    public String getQueryId() {
        return queryId;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public String getLogicName() {
        return logicName;
    }
    
    public void setLogicName(String logicName) {
        this.logicName = logicName;
    }
    
    public long getPageNumber() {
        return pageNumber;
    }
    
    public void setPageNumber(long pageNumber) {
        this.pageNumber = pageNumber;
    }
    
    public boolean isPartialResults() {
        return partialResults;
    }
    
    public void setPartialResults(boolean partialResults) {
        this.partialResults = partialResults;
    }
    
}

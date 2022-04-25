package datawave.webservice.result;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "CachedResultsResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class CachedResultsResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "OriginalQueryId")
    private String originalQueryId = null;
    
    @XmlElement(name = "QueryId")
    private String queryId = null;
    
    @XmlElement(name = "Alias")
    private String alias = null;
    
    @XmlElement(name = "ViewName")
    private String viewName = null;
    
    @XmlElement(name = "TotalRows")
    private Integer totalRows = null;
    
    public String getOriginalQueryId() {
        return originalQueryId;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public String getViewName() {
        return viewName;
    }
    
    public void setOriginalQueryId(String originalQueryId) {
        this.originalQueryId = originalQueryId;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    public void setViewName(String viewName) {
        this.viewName = viewName;
    }
    
    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }
    
    public Integer getTotalRows() {
        return totalRows;
    }
};

package datawave.webservice.result;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

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

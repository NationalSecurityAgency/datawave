package nsa.datawave.webservice.result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.exception.QueryExceptionType;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class BaseResponse implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "OperationTimeMS")
    private long operationTimeMS = 0;
    
    @XmlElement(name = "HasResults")
    private boolean hasResults = false;
    
    @XmlElement(name = "Messages")
    private List<String> messages = null;
    
    @XmlElementWrapper(name = "Exceptions")
    @XmlElement(name = "Exception")
    private LinkedList<QueryExceptionType> exceptions = null;
    
    public BaseResponse() {}
    
    public long getOperationTimeMS() {
        return operationTimeMS;
    }
    
    public List<String> getMessages() {
        return messages;
    }
    
    public List<QueryExceptionType> getExceptions() {
        return exceptions;
    }
    
    public void setOperationTimeMS(long operationTimeMS) {
        this.operationTimeMS = operationTimeMS;
    }
    
    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
    
    public void setExceptions(LinkedList<QueryExceptionType> exceptions) {
        this.exceptions = exceptions;
    }
    
    public boolean getHasResults() {
        return this.hasResults;
    }
    
    public void setHasResults(boolean hasResults) {
        this.hasResults = hasResults;
    }
    
    public void addMessage(String message) {
        if (this.messages == null) {
            this.messages = new ArrayList<String>();
        }
        this.messages.add(message);
    }
    
    public void addExceptions(List<QueryException> e) {
        if (null == e) {
            return;
        }
        
        for (QueryException qe : e) {
            addException(qe);
        }
    }
    
    public void addException(QueryException e) {
        if (null == e)
            return;
        if (this.exceptions == null) {
            this.exceptions = new LinkedList<QueryExceptionType>();
        }
        
        QueryExceptionType qet = new QueryExceptionType();
        if (null != e.getMessage())
            qet.setMessage(e.getMessage());
        
        if (null != e.getErrorCode())
            qet.setCode(e.getErrorCode());
        
        // Send the cause's message if present, or the cause itself since that
        // will contain the Exception's name
        if (null != e.getCause() && null != e.getCause().getMessage())
            qet.setCause(e.getCause().getMessage());
        else if (null != e.getCause())
            qet.setCause(e.getCause().toString());
        
        // Need to ensure that make the cause and exception of the QueryExceptionType be
        // non-null or else they won't be serialized into the returned Response
        if (qet.getCause() == null) {
            qet.setCause("Exception with no cause caught");
        }
        if (qet.getMessage() == null) {
            qet.setMessage("Exception with no message or cause message caught");
        }
        
        this.exceptions.push(qet);
    }
}

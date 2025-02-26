package datawave.webservice.result;

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

import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;

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
            this.messages = new ArrayList<>();
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
        addException(e, e.getErrorCode());
    }
    
    public void addException(Exception e) {
        if (e instanceof QueryException) {
            addException((QueryException) e);
        } else if (null != e) {
            addException(e, null);
        }
    }
    
    protected void addException(Exception e, String errorCode) {
        QueryExceptionType qet = new QueryExceptionType();
        
        // Prefer the cause's message if present, but in any case ensure that at least some message is filled in.
        if (null != e.getCause() && null != e.getCause().getMessage()) {
            qet.setMessage(e.getCause().getMessage());
        } else if (null != e.getMessage()) {
            qet.setMessage(e.getMessage());
        } else {
            qet.setMessage("Exception with no message or cause message caught");
        }
        
        // Fill in the cause or a default string otherwise so that we always send something in the response.
        if (null != e.getCause()) {
            qet.setCause(e.getCause().toString());
        } else {
            qet.setCause("Exception with no cause caught");
        }
        
        if (null != errorCode) {
            qet.setCode(errorCode);
        }
        
        if (this.exceptions == null) {
            this.exceptions = new LinkedList<>();
        }
        this.exceptions.push(qet);
    }
    
    @Override
    public String toString() {
        return "BaseResponse{" + "operationTimeMS=" + operationTimeMS + ", hasResults=" + hasResults + ", messages=" + messages + ", exceptions=" + exceptions
                        + '}';
    }
}

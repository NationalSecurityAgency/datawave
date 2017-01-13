package nsa.datawave.webservice.response;

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

import nsa.datawave.webservice.exception.AccumuloExceptionType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class BaseResponse implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "Messages")
    @XmlElement(name = "Message")
    private List<String> messages = null;
    
    @XmlElementWrapper(name = "Exceptions")
    @XmlElement(name = "Exception")
    private LinkedList<AccumuloExceptionType> exceptions = null;
    
    public BaseResponse() {}
    
    public List<String> getMessages() {
        return messages;
    }
    
    public List<AccumuloExceptionType> getExceptions() {
        return exceptions;
    }
    
    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
    
    public void setExceptions(LinkedList<AccumuloExceptionType> exceptions) {
        this.exceptions = exceptions;
    }
    
    public void addMessage(String message) {
        if (this.messages == null) {
            this.messages = new ArrayList<String>();
        }
        this.messages.add(message);
    }
    
    public void addException(Exception e) {
        if (null == e)
            return;
        if (this.exceptions == null) {
            this.exceptions = new LinkedList<AccumuloExceptionType>();
        }
        AccumuloExceptionType exceptionType = new AccumuloExceptionType();
        if (null != e.getMessage()) {
            exceptionType.setMessage(e.getMessage());
        }
        if (null != e.getCause() && null != e.getCause().getMessage()) {
            exceptionType.setCause(e.getCause().getMessage());
        }
        this.exceptions.push(exceptionType);
    }
}

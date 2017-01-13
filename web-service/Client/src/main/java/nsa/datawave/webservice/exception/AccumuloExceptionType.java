package nsa.datawave.webservice.exception;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class AccumuloExceptionType implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "Message")
    private String message;
    
    @XmlElement(name = "Cause")
    private String cause;
    
    public AccumuloExceptionType() {
        super();
    }
    
    public AccumuloExceptionType(String message, String cause) {
        super();
        this.message = message;
        this.cause = cause;
    }
    
    public String getMessage() {
        return message;
    }
    
    public String getCause() {
        return cause;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public void setCause(String cause) {
        this.cause = cause;
    }
}

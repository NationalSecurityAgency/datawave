package datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.response.objects.AuthorizationFailure;
import datawave.webservice.response.objects.ConstraintViolation;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "UpdateResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class UpdateResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(required = true)
    private Integer mutationsAccepted = null;
    
    @XmlElement(required = true)
    private Integer mutationsDenied = null;
    
    @XmlElementWrapper(name = "authorizationFailures")
    @XmlElement(name = "authorizationFailure")
    private List<AuthorizationFailure> authorizationFailures = null;
    
    @XmlElementWrapper(name = "constraintViolations")
    @XmlElement(name = "constraintViolation")
    private List<ConstraintViolation> constraintViolations = null;
    
    @XmlElement(name = "tableNotFound")
    private List<String> tableNotFoundList = null;
    
    public Integer getMutationsAccepted() {
        return mutationsAccepted;
    }
    
    public Integer getMutationsDenied() {
        return mutationsDenied;
    }
    
    public List<AuthorizationFailure> getAuthorizationFailures() {
        return authorizationFailures;
    }
    
    public List<ConstraintViolation> getConstraintViolations() {
        return constraintViolations;
    }
    
    public List<String> getTableNotFoundList() {
        return tableNotFoundList;
    }
    
    public void setMutationsAccepted(Integer mutationsAccepted) {
        this.mutationsAccepted = mutationsAccepted;
    }
    
    public void setMutationsDenied(Integer mutationsDenied) {
        this.mutationsDenied = mutationsDenied;
    }
    
    public void setAuthorizationFailures(List<AuthorizationFailure> authorizationFailures) {
        this.authorizationFailures = authorizationFailures;
    }
    
    public void setConstraintViolations(List<ConstraintViolation> constraintViolations) {
        this.constraintViolations = constraintViolations;
    }
    
    public void setTableNotFoundList(List<String> tableNotFoundList) {
        this.tableNotFoundList = tableNotFoundList;
    }
}

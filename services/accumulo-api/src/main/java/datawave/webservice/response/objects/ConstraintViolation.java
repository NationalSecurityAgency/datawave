package datawave.webservice.response.objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.NONE)
public class ConstraintViolation {
    
    @XmlAttribute(required = true)
    private String constraintClass = null;
    
    @XmlAttribute(required = true)
    private Integer violationCode = null;
    
    @XmlAttribute(required = true)
    private String description = null;
    
    @XmlAttribute(required = true)
    private String numberViolations = null;
    
    public String getConstraintClass() {
        return constraintClass;
    }
    
    public Integer getViolationCode() {
        return violationCode;
    }
    
    public String getDescription() {
        return description;
    }
    
    public String getNumberViolations() {
        return numberViolations;
    }
    
    public void setConstraintClass(String constraintClass) {
        this.constraintClass = constraintClass;
    }
    
    public void setViolationCode(Integer violationCode) {
        this.violationCode = violationCode;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public void setNumberViolations(String numberViolations) {
        this.numberViolations = numberViolations;
    }
    
}

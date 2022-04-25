package datawave.webservice.results.mr;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Algorithm")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class WorkflowAlgorithmDescription implements Serializable {
    private static final long serialVersionUID = -3497664868427218059L;
    
    @XmlAttribute(name = "name")
    protected String name = null;
    
    @XmlElement(name = "Description")
    protected String description = null;
    
    @JsonProperty(value = "AdditionalRequiredAlgorithmParameters")
    @XmlElementWrapper(name = "AdditionalRequiredAlgorithmParameters")
    @XmlElement(name = "Parameter")
    protected List<String> additionalRequiredAlgorithmParameters = null;
    
    @JsonProperty(value = "AdditionalOptionalAlgorithmParameters")
    @XmlElementWrapper(name = "AdditionalOptionalAlgorithmParameters")
    @XmlElement(name = "Parameter")
    protected List<String> additionalOptionalAlgorithmParameters = null;
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
    
    public List<String> getAdditionalRequiredAlgorithmParameters() {
        return additionalRequiredAlgorithmParameters;
    }
    
    public List<String> getAdditionalOptionalAlgorithmParameters() {
        return additionalOptionalAlgorithmParameters;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public void setAdditionalRequiredAlgorithmParameters(List<String> requiredParameters) {
        this.additionalRequiredAlgorithmParameters = requiredParameters;
    }
    
    public void setAdditionalOptionalAlgorithmParameters(List<String> optionalParameters) {
        this.additionalOptionalAlgorithmParameters = optionalParameters;
    }
    
}

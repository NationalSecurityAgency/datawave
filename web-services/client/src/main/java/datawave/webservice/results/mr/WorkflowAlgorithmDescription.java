package datawave.webservice.results.mr;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonProperty;

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

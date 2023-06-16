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

@XmlRootElement(name = "MapReduceJobDescription")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MapReduceJobDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "name", required = true)
    protected String name = null;

    @XmlElement(name = "Description")
    protected String description = null;

    @XmlElement(name = "JobType", required = true)
    protected String jobType = null;

    @JsonProperty(value = "RequiredRuntimeParameters")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "RequiredRuntimeParameters")
    @XmlElement(name = "Parameter")
    protected List<String> requiredRuntimeParameters = null;

    @JsonProperty(value = "OptionalRuntimeParameters")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "OptionalRuntimeParameters")
    @XmlElement(name = "Parameter")
    protected List<String> optionalRuntimeParameters = null;

    @JsonProperty(value = "WorkflowAlgorithmDescriptions")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "WorkflowAlgorithmDescriptions")
    @XmlElement(name = "Algorithm")
    List<WorkflowAlgorithmDescription> workflowAlgorithmDescriptions = null;

    public String getName() {
        return name;
    }

    public String getJobType() {
        return jobType;
    }

    public String getDescription() {
        return description;
    }

    public List<String> getRequiredRuntimeParameters() {
        return requiredRuntimeParameters;
    }

    public List<String> getOptionalRuntimeParameters() {
        return optionalRuntimeParameters;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setRequiredRuntimeParameters(List<String> requiredRuntimeParameters) {
        this.requiredRuntimeParameters = requiredRuntimeParameters;
    }

    public void setOptionalRuntimeParameters(List<String> optionalRuntimeParameters) {
        this.optionalRuntimeParameters = optionalRuntimeParameters;
    }

    public List<WorkflowAlgorithmDescription> getWorkflowAlgorithmDescriptions() {
        return workflowAlgorithmDescriptions;
    }

    public void setWorkflowAlgorithmDescriptions(List<WorkflowAlgorithmDescription> workflowAlgorithmDescriptions) {
        this.workflowAlgorithmDescriptions = workflowAlgorithmDescriptions;
    }

    public WorkflowAlgorithmDescription getNewWorkflowAlgorithmDescription(String name) {
        WorkflowAlgorithmDescription wfad = new WorkflowAlgorithmDescription();
        wfad.setName(name);
        return wfad;
    }

}

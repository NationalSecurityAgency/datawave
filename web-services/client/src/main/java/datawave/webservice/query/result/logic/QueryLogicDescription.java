package datawave.webservice.query.result.logic;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import com.fasterxml.jackson.annotation.JsonProperty;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryLogicDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    public QueryLogicDescription() {}

    @XmlAttribute(name = "name", required = true)
    private String name = null;

    @XmlElement(name = "LogicDescription")
    private String logicDescription = "Not configured";

    @XmlElement(name = "AuditType")
    private String auditType = null;

    @XmlElement(name = "ResponseClass")
    private String responseClass = null;

    @XmlElementWrapper(name = "RequiredRoles")
    @XmlElement(name = "Role")
    private List<String> requiredRoles = null;

    @XmlElementWrapper(name = "SupportedQuerySyntax")
    @XmlElement(name = "Syntax")
    private List<String> querySyntax = null;

    @JsonProperty("SupportedParameters")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "SupportedParameters")
    @XmlElement(name = "Parameter")
    private List<String> supportedParams = null;

    @JsonProperty("RequiredParameters")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "RequiredParameters")
    @XmlElement(name = "Parameter")
    private List<String> requiredParams = null;

    @XmlElementWrapper(name = "ExampleQueries")
    @XmlElement(name = "Query")
    private List<String> exampleQueries = null;

    public QueryLogicDescription(String name) {
        this.name = name;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public String getLogicDescription() {
        return logicDescription;
    }

    public String getAuditType() {
        return auditType;
    }

    public void setLogicDescription(String logicDescription) {
        this.logicDescription = logicDescription;
    }

    public void setAuditType(String auditType) {
        this.auditType = auditType;
    }

    public String getResponseClass() {
        return responseClass;
    }

    public void setResponseClass(String responseClass) {
        this.responseClass = responseClass;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getQuerySyntax() {
        return querySyntax;
    }

    public void setQuerySyntax(List<String> querySyntax) {
        this.querySyntax = querySyntax;
    }

    public List<String> getRequiredRoles() {
        return requiredRoles;
    }

    public void setRequiredRoles(List<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }

    public List<String> getSupportedParams() {
        return supportedParams;
    }

    public void setSupportedParams(List<String> supportedParams) {
        this.supportedParams = supportedParams;
    }

    public List<String> getRequiredParams() {
        return requiredParams;
    }

    public void setRequiredParams(List<String> requiredParams) {
        this.requiredParams = requiredParams;
    }

    public List<String> getExampleQueries() {
        return exampleQueries;
    }

    public void setExampleQueries(List<String> exampleQueries) {
        this.exampleQueries = exampleQueries;
    }

}

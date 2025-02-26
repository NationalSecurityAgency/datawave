package datawave.microservice.query.mapreduce.config;

import java.util.List;
import java.util.Map;

import datawave.microservice.config.accumulo.AccumuloProperties;

public class MapReduceJobProperties {
    private String startingClass = null;
    private List<String> basePackages = null;
    private String description = null;
    protected String hdfsUri = null;
    protected String jobTracker = null;
    protected String queueName = null;
    protected String jobJarName = null;
    protected Map<String,Class<?>> requiredParameters = null;
    protected Map<String,Class<?>> requiredRuntimeParameters = null;
    protected Map<String,Class<?>> optionalRuntimeParameters = null;
    private List<String> requiredRoles = null;
    private List<String> requiredAuths = null;
    protected Map<String,String> jobConfigurationProperties = null;
    protected Map<String,String> jobSystemProperties = null;
    
    protected String jobType = "mapreduce";
    
    protected AccumuloProperties accumulo;
    
    public String getStartingClass() {
        return startingClass;
    }
    
    public void setStartingClass(String startingClass) {
        this.startingClass = startingClass;
    }
    
    public List<String> getBasePackages() {
        return basePackages;
    }
    
    public void setBasePackages(List<String> basePackages) {
        this.basePackages = basePackages;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getHdfsUri() {
        return hdfsUri;
    }
    
    public void setHdfsUri(String hdfsUri) {
        this.hdfsUri = hdfsUri;
    }
    
    public String getJobTracker() {
        return jobTracker;
    }
    
    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
    
    public String getJobJarName() {
        return jobJarName;
    }
    
    public void setJobJarName(String jobJarName) {
        this.jobJarName = jobJarName;
    }
    
    public Map<String,Class<?>> getRequiredParameters() {
        return requiredParameters;
    }
    
    public void setRequiredParameters(Map<String,Class<?>> requiredParameters) {
        this.requiredParameters = requiredParameters;
    }
    
    public Map<String,Class<?>> getRequiredRuntimeParameters() {
        return requiredRuntimeParameters;
    }
    
    public void setRequiredRuntimeParameters(Map<String,Class<?>> requiredRuntimeParameters) {
        this.requiredRuntimeParameters = requiredRuntimeParameters;
    }
    
    public Map<String,Class<?>> getOptionalRuntimeParameters() {
        return optionalRuntimeParameters;
    }
    
    public void setOptionalRuntimeParameters(Map<String,Class<?>> optionalRuntimeParameters) {
        this.optionalRuntimeParameters = optionalRuntimeParameters;
    }
    
    public List<String> getRequiredRoles() {
        return requiredRoles;
    }
    
    public void setRequiredRoles(List<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }
    
    public List<String> getRequiredAuths() {
        return requiredAuths;
    }
    
    public void setRequiredAuths(List<String> requiredAuths) {
        this.requiredAuths = requiredAuths;
    }
    
    public Map<String,String> getJobConfigurationProperties() {
        return jobConfigurationProperties;
    }
    
    public void setJobConfigurationProperties(Map<String,String> jobConfigurationProperties) {
        this.jobConfigurationProperties = jobConfigurationProperties;
    }
    
    public Map<String,String> getJobSystemProperties() {
        return jobSystemProperties;
    }
    
    public void setJobSystemProperties(Map<String,String> jobSystemProperties) {
        this.jobSystemProperties = jobSystemProperties;
    }
    
    public String getJobType() {
        return jobType;
    }
    
    public void setJobType(String jobType) {
        this.jobType = jobType;
    }
    
    public AccumuloProperties getAccumulo() {
        return accumulo;
    }
    
    public void setAccumulo(AccumuloProperties accumulo) {
        this.accumulo = accumulo;
    }
}

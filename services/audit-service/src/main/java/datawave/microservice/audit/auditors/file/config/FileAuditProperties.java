package datawave.microservice.audit.auditors.file.config;

import datawave.microservice.audit.config.AuditProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;

@Validated
public class FileAuditProperties {
    
    @NotEmpty
    private String pathUri;
    private String subpath;
    private String subpathEnvVar;
    private String prefix;
    
    @DecimalMin("10485760")
    private Long maxFileLenBytes;
    
    @DecimalMin("0")
    private Long maxFileAgeMillis;
    
    @Valid
    private AuditProperties.Filesystem fs = new AuditProperties.Filesystem();
    
    public String getPathUri() {
        return pathUri;
    }
    
    public void setPathUri(String pathUri) {
        this.pathUri = pathUri;
    }
    
    public String getSubpath() {
        return subpath;
    }
    
    public void setSubpath(String subpath) {
        this.subpath = subpath;
    }
    
    public String getSubpathEnvVar() {
        return subpathEnvVar;
    }
    
    public void setSubpathEnvVar(String subpathEnvVar) {
        this.subpathEnvVar = subpathEnvVar;
    }
    
    public String getPrefix() {
        return prefix;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public Long getMaxFileLenBytes() {
        return maxFileLenBytes;
    }
    
    public void setMaxFileLenBytes(Long maxFileLenBytes) {
        this.maxFileLenBytes = maxFileLenBytes;
    }
    
    public Long getMaxFileAgeMillis() {
        return maxFileAgeMillis;
    }
    
    public void setMaxFileAgeMillis(Long maxFileAgeMillis) {
        this.maxFileAgeMillis = maxFileAgeMillis;
    }
    
    public AuditProperties.Filesystem getFs() {
        return fs;
    }
    
    public void setFs(AuditProperties.Filesystem fs) {
        this.fs = fs;
    }
}

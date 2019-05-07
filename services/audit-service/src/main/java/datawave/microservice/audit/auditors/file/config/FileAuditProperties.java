package datawave.microservice.audit.auditors.file.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Validated
public class FileAuditProperties {
    
    @NotEmpty
    private String pathUri;
    private String subPath;
    private String subPathEnvVar;
    private String prefix;
    private List<String> fsConfigResources;
    
    @DecimalMin("10")
    private Long maxFileLengthMB;
    
    @DecimalMin("60")
    private Long maxFileAgeSeconds;
    
    public String getPathUri() {
        return pathUri;
    }
    
    public void setPathUri(String pathUri) {
        this.pathUri = pathUri;
    }
    
    public String getSubPath() {
        return subPath;
    }
    
    public void setSubPath(String subPath) {
        this.subPath = subPath;
    }
    
    public String getSubPathEnvVar() {
        return subPathEnvVar;
    }
    
    public void setSubPathEnvVar(String subPathEnvVar) {
        this.subPathEnvVar = subPathEnvVar;
    }
    
    public String getPrefix() {
        return prefix;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public List<String> getFsConfigResources() {
        return fsConfigResources;
    }
    
    public void setFsConfigResources(List<String> fsConfigResources) {
        this.fsConfigResources = fsConfigResources;
    }
    
    public Long getMaxFileLengthMB() {
        return maxFileLengthMB;
    }
    
    public void setMaxFileLengthMB(Long maxFileLengthMB) {
        this.maxFileLengthMB = maxFileLengthMB;
    }
    
    public Long getMaxFileAgeSeconds() {
        return maxFileAgeSeconds;
    }
    
    public void setMaxFileAgeSeconds(Long maxFileAgeSeconds) {
        this.maxFileAgeSeconds = maxFileAgeSeconds;
    }
}

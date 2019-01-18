package datawave.microservice.audit.auditors.file.config;

import datawave.microservice.audit.config.AuditProperties;

public class FileAuditProperties {
    
    private String path;
    private String prefix;
    private Long maxFileLenBytes;
    private Long maxFileAgeMillis;
    private AuditProperties.Filesystem fs = new AuditProperties.Filesystem();
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
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

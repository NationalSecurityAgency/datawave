package datawave.microservice.fileProvider.config;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Validated
@Configuration
@ConfigurationProperties(prefix = "file-provider.files")
public class FileConfigProperties {
    @Valid
    private List<FileConfig> files;
    
    public List<FileConfig> getFiles() {
        return files;
    }
    
    public void setFiles(List<FileConfig> files) {
        this.files = files;
    }
    
    @Validated
    public class FileConfig {
        @NotBlank
        private String label;
        @NotBlank
        private String name;
        @Valid
        private DownloadConfig download;
        
        // Getters and Setters
        public String getLabel() {
            return label;
        }
        
        public void setLabel(String label) {
            this.label = label;
        }
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public DownloadConfig getDownload() {
            return download;
        }
        
        public void setDownload(DownloadConfig download) {
            this.download = download;
        }
    }
    
    @Validated
    public class DownloadConfig {
        @NotBlank
        private String method;
        private String source;
        private String schedule;
        
        public String getMethod() {
            return method;
        }
        
        public void setMethod(String method) {
            this.method = method;
        }
        
        public String getSource() {
            return source;
        }
        
        public void setSource(String source) {
            this.source = source;
        }
        
        public String getSchedule() {
            return schedule;
        }
        
        public void setSchedule(String schedule) {
            this.schedule = schedule;
        }
    }
}

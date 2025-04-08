package datawave.microservice.fileProvider.config;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.validation.annotation.Validated;

@Validated
@Configuration
@ConfigurationProperties(prefix = "file-provider.files")
public class FileConfigProperties {
    
    // Note from Laura:
    // It's possible that this overall layout is too simplistic for our needs, especially if we need to support multiple methods of fetching a file,
    // e.g. https, ftp, scp, etc. Perhaps DownloadConfig should be an abstract class with different implementations for each method type with
    // method-specific properties that need to be given. Would need to look into polymorphic bean instantiation for the config classes via Spring.
    
    @Valid
    private List<FileConfig> files; //all of the files that are part of the config; ie the ones we're going to try and download and save into here
    
    public List<FileConfig> getFiles() {
        return files;
    }
    
    public void setFiles(List<FileConfig> files) {
        this.files = files;
    }
    
    @Validated
    public static class FileConfig { // outlines the files we want to make, and how we'll get their contents (download)
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
    public static class DownloadConfig { // how we'll download the files and finally save their contents into the FileConfig this is a member of
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

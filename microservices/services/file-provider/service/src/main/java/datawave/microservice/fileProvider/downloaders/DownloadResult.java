package datawave.microservice.fileProvider.downloaders;

import java.util.List;

// Could be extended by subtypes to contain additional details for each method, e.g. a HttpsDownloadResult could have a method to return the http code
public class DownloadResult {
    
    // potential status definitions
    public enum Status {
        PENDING,
        IN_PROGRESS,
        COMPLETE,
        ERROR
    }
    
    protected Status status;
    protected List<String> messages;
    
    public Status getStatus() {
        return status;
    }
    
    public List<String> getMessages() {
        return messages;
    }
}

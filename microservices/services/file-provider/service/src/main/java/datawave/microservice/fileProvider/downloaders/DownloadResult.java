package datawave.microservice.fileProvider.downloaders;

import java.util.List;
import java.util.Map;

// Could be extended by subtypes to contain additional details for each method, e.g. a HttpsDownloadResult could have a method to return the http code

//only created and returned at the end of the download atm. No async stuff yet.
public class DownloadResult {

    /**
     * TODO: We need to make a list of all issues we want to catch vs which ones we just want to say "whoops something happened!"
     */

    // potential status definitions
    public enum Status {
        COMPLETE,
        ERROR
    }

    protected Status status;
    protected List<String> messages;
    protected Map<String,String> properties;

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Status getStatus() {
        return status;
    }

    public List<String> getMessages() {
        return messages;
    }

    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }
}

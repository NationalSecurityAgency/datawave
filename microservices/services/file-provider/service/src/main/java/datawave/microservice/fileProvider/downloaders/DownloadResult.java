package datawave.microservice.fileProvider.downloaders;

import java.util.List;
import java.util.Map;

// Could be extended by subtypes to contain additional details for each method, e.g. a HttpsDownloadResult could have a method to return the http code
public class DownloadResult {

    protected List<String> messages;
    protected Map<String,String> properties;

    public List<String> getMessages() {
        return messages;
    }

    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }
}

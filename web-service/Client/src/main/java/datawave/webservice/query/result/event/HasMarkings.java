package datawave.webservice.query.result.event;

import java.util.Map;

public interface HasMarkings {
    
    void setMarkings(Map<String,String> markings);
    
    Map<String,String> getMarkings();
}

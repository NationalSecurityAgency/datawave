package datawave.webservice.response.objects;

import datawave.webservice.query.result.event.HasMarkings;

/**
 * 
 */
public interface KeyInterface extends HasMarkings {
    
    String getRow();
    
    String getColFam();
    
    String getColQual();
    
    Long getTimestamp();
    
}

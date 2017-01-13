package nsa.datawave.webservice.response.objects;

import nsa.datawave.webservice.query.result.event.HasMarkings;

/**
 * 
 */
public interface KeyInterface extends HasMarkings {
    
    public String getRow();
    
    public String getColFam();
    
    public String getColQual();
    
    public Long getTimestamp();
    
}

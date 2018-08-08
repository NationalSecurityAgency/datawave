package datawave.webservice.query.result.event;

import java.util.List;

public interface Fielded extends HasMarkings {
    
    List getFields();
    
    void setFields(List fields);
}

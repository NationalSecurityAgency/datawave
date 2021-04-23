package datawave.webservice.mr.configuration;

import datawave.microservice.query.logic.QueryLogicFactory;

public interface NeedQueryLogicFactory {
    
    void setQueryLogicFactory(QueryLogicFactory factory);
    
}

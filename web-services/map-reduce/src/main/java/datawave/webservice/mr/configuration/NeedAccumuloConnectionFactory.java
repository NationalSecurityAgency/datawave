package datawave.webservice.mr.configuration;

import datawave.microservice.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {
    
    void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);
    
}

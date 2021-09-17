package datawave.webservice.mr.configuration;

import datawave.services.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {
    
    void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);
    
}

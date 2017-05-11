package datawave.webservice.mr.configuration;

import datawave.webservice.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {
    
    public void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);
    
}

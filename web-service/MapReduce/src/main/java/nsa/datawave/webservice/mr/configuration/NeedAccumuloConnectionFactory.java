package nsa.datawave.webservice.mr.configuration;

import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {
    
    public void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);
    
}

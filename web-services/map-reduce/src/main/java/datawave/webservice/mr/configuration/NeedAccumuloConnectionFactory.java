package datawave.webservice.mr.configuration;

import datawave.webservice.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {

    void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);

}

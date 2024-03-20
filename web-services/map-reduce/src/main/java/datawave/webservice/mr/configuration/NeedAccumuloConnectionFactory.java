package datawave.webservice.mr.configuration;

import datawave.core.common.connection.AccumuloConnectionFactory;

public interface NeedAccumuloConnectionFactory {

    void setAccumuloConnectionFactory(AccumuloConnectionFactory factory);

}

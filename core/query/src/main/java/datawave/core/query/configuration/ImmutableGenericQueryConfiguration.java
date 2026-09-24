package datawave.core.query.configuration;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;

import datawave.microservice.query.Query;

public interface ImmutableGenericQueryConfiguration {
    Collection<QueryData> getQueries();

    Iterator<QueryData> getQueriesIter();

    boolean isCheckpointable();

    AccumuloClient getClient();

    Query getQuery();

    String getQueryString();

    Set<String> getAuths();

    Set<Authorizations> getAuthorizations();

    int getBaseIteratorPriority();

    Date getBeginDate();

    Date getEndDate();

    Long getMaxWork();

    String getTableName();

    boolean getBypassAccumulo();

    String getAccumuloPassword();

    boolean isReduceResults();

    String getConnPoolName();

    Map<String,ScannerBase.ConsistencyLevel> getTableConsistencyLevels();

    Map<String,Map<String,String>> getTableHints();

    boolean canRunQuery();
}
